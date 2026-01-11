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

#include "velox/connectors/lakehouse/iceberg/IcebergConfig.h"

#include <boost/algorithm/string.hpp>

namespace facebook::velox::connector::lakehouse::iceberg {

namespace {

IcebergConfig::InsertExistingPartitionsBehavior
stringToInsertExistingPartitionsBehavior(const std::string& strValue) {
  auto upperValue = boost::algorithm::to_upper_copy(strValue);
  if (upperValue == "ERROR") {
    return IcebergConfig::InsertExistingPartitionsBehavior::kError;
  }
  if (upperValue == "OVERWRITE") {
    return IcebergConfig::InsertExistingPartitionsBehavior::kOverwrite;
  }
  VELOX_UNSUPPORTED(
      "Unsupported insert existing partitions behavior: {}.", strValue);
}

} // namespace

// static
std::string IcebergConfig::insertExistingPartitionsBehaviorString(
    InsertExistingPartitionsBehavior behavior) {
  switch (behavior) {
    case InsertExistingPartitionsBehavior::kError:
      return "ERROR";
    case InsertExistingPartitionsBehavior::kOverwrite:
      return "OVERWRITE";
    default:
      return fmt::format("UNKNOWN BEHAVIOR {}", static_cast<int>(behavior));
  }
}

IcebergConfig::InsertExistingPartitionsBehavior
IcebergConfig::insertExistingPartitionsBehavior(
    const config::ConfigBase* session) const {
  return stringToInsertExistingPartitionsBehavior(session->get<std::string>(
      kInsertExistingPartitionsBehaviorSession,
      config_->get<std::string>(kInsertExistingPartitionsBehavior, "ERROR")));
}

uint32_t IcebergConfig::maxPartitionsPerWriters(
    const config::ConfigBase* session) const {
  return session->get<uint32_t>(
      kMaxPartitionsPerWritersSession,
      config_->get<uint32_t>(kMaxPartitionsPerWriters, 128));
}

uint32_t IcebergConfig::maxBucketCount(const config::ConfigBase* session) const {
  return session->get<uint32_t>(
      kMaxBucketCountSession, config_->get<uint32_t>(kMaxBucketCount, 100'000));
}

bool IcebergConfig::immutablePartitions() const {
  return config_->get<bool>(kImmutablePartitions, false);
}

std::string IcebergConfig::gcsEndpoint() const {
  return config_->get<std::string>(kGcsEndpoint, std::string(""));
}

std::string IcebergConfig::gcsCredentialsPath() const {
  return config_->get<std::string>(kGcsCredentialsPath, std::string(""));
}

std::optional<int> IcebergConfig::gcsMaxRetryCount() const {
  return static_cast<std::optional<int>>(config_->get<int>(kGcsMaxRetryCount));
}

std::optional<std::string> IcebergConfig::gcsMaxRetryTime() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kGcsMaxRetryTime));
}

bool IcebergConfig::isOrcUseColumnNames(const config::ConfigBase* session) const {
  return session->get<bool>(
      kOrcUseColumnNamesSession, config_->get<bool>(kOrcUseColumnNames, false));
}

bool IcebergConfig::isParquetUseColumnNames(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kParquetUseColumnNamesSession,
      config_->get<bool>(kParquetUseColumnNames, false));
}

bool IcebergConfig::isFileColumnNamesReadAsLowerCase(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kFileColumnNamesReadAsLowerCaseSession,
      config_->get<bool>(kFileColumnNamesReadAsLowerCase, false));
}

bool IcebergConfig::isPartitionPathAsLowerCase(
    const config::ConfigBase* session) const {
  return session->get<bool>(kPartitionPathAsLowerCaseSession, true);
}

bool IcebergConfig::allowNullPartitionKeys(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kAllowNullPartitionKeysSession,
      config_->get<bool>(kAllowNullPartitionKeys, true));
}

bool IcebergConfig::ignoreMissingFiles(const config::ConfigBase* session) const {
  return session->get<bool>(kIgnoreMissingFilesSession, false);
}

int64_t IcebergConfig::maxCoalescedBytes(const config::ConfigBase* session) const {
  return session->get<int64_t>(
      kMaxCoalescedBytesSession,
      config_->get<int64_t>(kMaxCoalescedBytes, 128 << 20)); // 128MB
}

int32_t IcebergConfig::maxCoalescedDistanceBytes(
    const config::ConfigBase* session) const {
  const auto distance = config::toCapacity(
      session->get<std::string>(
          kMaxCoalescedDistanceSession,
          config_->get<std::string>(kMaxCoalescedDistance, "512kB")),
      config::CapacityUnit::BYTE);
  VELOX_USER_CHECK_LE(
      distance,
      std::numeric_limits<int32_t>::max(),
      "The max merge distance to combine read requests must be less than 2GB."
      " Got {} bytes.",
      distance);
  return int32_t(distance);
}

int32_t IcebergConfig::prefetchRowGroups() const {
  return config_->get<int32_t>(kPrefetchRowGroups, 1);
}

int32_t IcebergConfig::loadQuantum(const config::ConfigBase* session) const {
  return session->get<int32_t>(
      kLoadQuantumSession, config_->get<int32_t>(kLoadQuantum, 8 << 20));
}

int32_t IcebergConfig::numCacheFileHandles() const {
  return config_->get<int32_t>(kNumCacheFileHandles, 20'000);
}

uint64_t IcebergConfig::fileHandleExpirationDurationMs() const {
  return config_->get<uint64_t>(kFileHandleExpirationDurationMs, 0);
}

bool IcebergConfig::isFileHandleCacheEnabled() const {
  return config_->get<bool>(kEnableFileHandleCache, true);
}

std::string IcebergConfig::writeFileCreateConfig() const {
  return config_->get<std::string>(kWriteFileCreateConfig, "");
}

uint32_t IcebergConfig::sortWriterMaxOutputRows(
    const config::ConfigBase* session) const {
  return session->get<uint32_t>(
      kSortWriterMaxOutputRowsSession,
      config_->get<uint32_t>(kSortWriterMaxOutputRows, 1024));
}

uint64_t IcebergConfig::sortWriterMaxOutputBytes(
    const config::ConfigBase* session) const {
  return config::toCapacity(
      session->get<std::string>(
          kSortWriterMaxOutputBytesSession,
          config_->get<std::string>(kSortWriterMaxOutputBytes, "10MB")),
      config::CapacityUnit::BYTE);
}

uint64_t IcebergConfig::sortWriterFinishTimeSliceLimitMs(
    const config::ConfigBase* session) const {
  return session->get<uint64_t>(
      kSortWriterFinishTimeSliceLimitMsSession,
      config_->get<uint64_t>(kSortWriterFinishTimeSliceLimitMs, 5'000));
}

uint64_t IcebergConfig::footerEstimatedSize() const {
  return config_->get<uint64_t>(kFooterEstimatedSize, 256UL << 10);
}

uint64_t IcebergConfig::filePreloadThreshold() const {
  return config_->get<uint64_t>(kFilePreloadThreshold, 8UL << 20);
}

uint8_t IcebergConfig::readTimestampUnit(const config::ConfigBase* session) const {
  const auto unit = session->get<uint8_t>(
      kReadTimestampUnitSession,
      config_->get<uint8_t>(kReadTimestampUnit, 3 /*milli*/));
  VELOX_CHECK(
      unit == 3 || unit == 6 /*micro*/ || unit == 9 /*nano*/,
      "Invalid timestamp unit.");
  return unit;
}

bool IcebergConfig::readTimestampPartitionValueAsLocalTime(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kReadTimestampPartitionValueAsLocalTimeSession,
      config_->get<bool>(kReadTimestampPartitionValueAsLocalTime, true));
}

bool IcebergConfig::readStatsBasedFilterReorderDisabled(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kReadStatsBasedFilterReorderDisabledSession,
      config_->get<bool>(kReadStatsBasedFilterReorderDisabled, false));
}

bool IcebergConfig::isRequestedTypeCheckEnabled(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kEnableRequestedTypeCheckSession,
      config_->get<bool>(kEnableRequestedTypeCheck, true));
}

std::string IcebergConfig::icebergLocalDataPath() const {
  return config_->get<std::string>(kLocalDataPath, "");
}

std::string IcebergConfig::icebergLocalFileFormat() const {
  return config_->get<std::string>(kLocalFileFormat, "");
}
} // namespace facebook::velox::connector::lakehouse::iceberg
