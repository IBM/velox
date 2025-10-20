/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/connectors/hive/storage_adapters/s3fs/S3ReadFile.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Counters.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

namespace {

template <typename T>
bool shouldRetryRequest(
    const T& outcome,
    uint32_t maxAttempts,
    uint32_t& retries,
    uint32_t& backoffMs) {
  if (outcome.IsSuccess()) {
    return false;
  }
  Aws::Client::AWSError<Aws::S3::S3Errors> error;
  auto s3ClientRetryCount = outcome.GetRetryCount();
  if (s3ClientRetryCount > 0) {
    retries += s3ClientRetryCount;
  }
  error = outcome.GetError();
  if (retries < maxAttempts &&
      error.GetResponseCode() ==
          Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE) {
    std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
    RECORD_METRIC_VALUE(facebook::velox::filesystems::kMetricS3ClientRetries);
    // If s3ClientRetryCount was non-zero we still need to count
    // client retries.
    ++retries;
    backoffMs *= 2;
    return true;
  }
  return false;
}

} // namespace

namespace facebook::velox::filesystems {

namespace {

// By default, the AWS SDK reads object data into an auto-growing StringStream.
// To avoid copies, read directly into a pre-allocated buffer instead.
// See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
// functionally similar recipe.
Aws::IOStreamFactory AwsWriteableStreamFactory(void* data, int64_t nbytes) {
  return [=]() { return Aws::New<StringViewStream>("", data, nbytes); };
}

} // namespace

class S3ReadFile ::Impl {
 public:
  explicit Impl(
      std::string_view path,
      Aws::S3::S3Client* client,
      uint32_t maxAttempts)
      : client_(client), maxAttempts_(maxAttempts) {
    getBucketAndKeyFromPath(path, bucket_, key_);
  }

  // Gets the length of the file.
  // Checks if there are any issues reading the file.
  void initialize(const filesystems::FileOptions& options) {
    if (options.fileSize.has_value()) {
      VELOX_CHECK_GE(
          options.fileSize.value(), 0, "File size must be non-negative");
      length_ = options.fileSize.value();
    }

    // Make it a no-op if invoked twice.
    if (length_ != -1) {
      return;
    }

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(awsString(bucket_));
    request.SetKey(awsString(key_));

    Aws::S3::Model::HeadObjectOutcome outcome;
    uint32_t retries = 0;
    uint32_t backoffMs = 100;

    do {
      outcome = client_->HeadObject(request);
    } while (shouldRetryRequest<Aws::S3::Model::HeadObjectOutcome>(
        outcome, maxAttempts_, retries, backoffMs));

    RECORD_METRIC_VALUE(kMetricS3MetadataCalls);
    if (!outcome.IsSuccess()) {
      RECORD_METRIC_VALUE(kMetricS3GetMetadataErrors);
    }

    RECORD_METRIC_VALUE(kMetricS3GetMetadataRetries, retries);
    VELOX_CHECK_AWS_OUTCOME(
        outcome, "Failed to get metadata for S3 object", bucket_, key_);
    length_ = outcome.GetResult().GetContentLength();
    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      const FileIoContext& context) const {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string
  pread(uint64_t offset, uint64_t length, const FileIoContext& context) const {
    std::string result(length, 0);
    char* position = result.data();
    preadInternal(offset, length, position);
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context) const {
    // 'buffers' contains Ranges(data, size)  with some gaps (data = nullptr) in
    // between. This call must populate the ranges (except gap ranges)
    // sequentially starting from 'offset'. AWS S3 GetObject does not support
    // multi-range. AWS S3 also charges by number of read requests and not size.
    // The idea here is to use a single read spanning all the ranges and then
    // populate individual ranges. We pre-allocate a buffer to support this.
    size_t length = 0;
    for (const auto range : buffers) {
      length += range.size();
    }
    // TODO: allocate from a memory pool
    std::string result(length, 0);
    preadInternal(offset, length, static_cast<char*>(result.data()));
    size_t resultOffset = 0;
    for (auto range : buffers) {
      if (range.data()) {
        memcpy(range.data(), &(result.data()[resultOffset]), range.size());
      }
      resultOffset += range.size();
    }
    return length;
  }

  uint64_t size() const {
    return length_;
  }

  uint64_t memoryUsage() const {
    // TODO: Check if any buffers are being used by the S3 library
    return sizeof(Aws::S3::S3Client) + kS3MaxKeySize + 2 * sizeof(std::string) +
        sizeof(int64_t);
  }

  bool shouldCoalesce() const {
    return false;
  }

  std::string getName() const {
    return fmt::format("s3://{}/{}", bucket_, key_);
  }

 private:
  // The assumption here is that "position" has space for at least "length"
  // bytes.
  void preadInternal(uint64_t offset, uint64_t length, char* position) const {
    // Read the desired range of bytes.
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::Model::GetObjectResult result;

    request.SetBucket(awsString(bucket_));
    request.SetKey(awsString(key_));
    std::stringstream ss;
    ss << "bytes=" << offset << "-" << offset + length - 1;
    request.SetRange(awsString(ss.str()));
    request.SetResponseStreamFactory(
        AwsWriteableStreamFactory(position, length));
    RECORD_METRIC_VALUE(kMetricS3ActiveConnections);
    RECORD_METRIC_VALUE(kMetricS3GetObjectCalls);
    Aws::S3::Model::GetObjectOutcome outcome;
    uint32_t retries = 0;
    uint32_t backoffMs = 100;

    do {
      outcome = client_->GetObject(request);
    } while (shouldRetryRequest<Aws::S3::Model::GetObjectOutcome>(
        outcome, maxAttempts_, retries, backoffMs));

    if (!outcome.IsSuccess()) {
      RECORD_METRIC_VALUE(kMetricS3GetObjectErrors);
    }
    RECORD_METRIC_VALUE(kMetricS3GetObjectRetries, retries);
    RECORD_METRIC_VALUE(kMetricS3ActiveConnections, -1);
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to get S3 object", bucket_, key_);
  }

  Aws::S3::S3Client* client_;
  std::string bucket_;
  std::string key_;
  int64_t length_ = -1;
  uint32_t maxAttempts_;
};

S3ReadFile::S3ReadFile(
    std::string_view path,
    Aws::S3::S3Client* client,
    uint32_t maxAttempts) {
  impl_ = std::make_shared<Impl>(path, client, maxAttempts);
}

S3ReadFile::~S3ReadFile() = default;

void S3ReadFile::initialize(const filesystems::FileOptions& options) {
  return impl_->initialize(options);
}

std::string_view S3ReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buf,
    const FileIoContext& context) const {
  return impl_->pread(offset, length, buf, context);
}

std::string S3ReadFile::pread(
    uint64_t offset,
    uint64_t length,
    const FileIoContext& context) const {
  return impl_->pread(offset, length, context);
}

uint64_t S3ReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const FileIoContext& context) const {
  return impl_->preadv(offset, buffers, context);
}

uint64_t S3ReadFile::size() const {
  return impl_->size();
}

uint64_t S3ReadFile::memoryUsage() const {
  return impl_->memoryUsage();
}

bool S3ReadFile::shouldCoalesce() const {
  return impl_->shouldCoalesce();
}

std::string S3ReadFile::getName() const {
  return impl_->getName();
}

} // namespace facebook::velox::filesystems
