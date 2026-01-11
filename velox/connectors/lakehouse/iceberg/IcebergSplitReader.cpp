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

#include "IcebergSplitReader.h"

#include "IcebergConnectorSplit.h"
#include "IcebergConnectorUtil.h"
#include "IcebergDeleteFile.h"
#include "IcebergTableHandle.h"
#include "velox/dwio/common/BufferUtil.h"

using namespace facebook::velox::dwio::common;

namespace facebook::velox::connector::lakehouse::iceberg  {

IcebergSplitReader::IcebergSplitReader(
    const std::shared_ptr<const ConnectorSplitBase>& split,
    const std::shared_ptr<const TableHandleBase>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<const ColumnHandleBase>>* partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const ConnectorConfigBase>& ConnectorConfigBase,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const std::shared_ptr<filesystems::File::IoStats>& fsStats,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<velox::common::ScanSpec>& scanSpec)
    : SplitReaderBase(
          split,
          tableHandle,
          partitionKeys,
          connectorQueryCtx,
          ConnectorConfigBase,
          readerOutputType,
          ioStats,
          fsStats,
          fileHandleFactory,
          executor,
          scanSpec),
      baseReadOffset_(0),
      splitOffset_(0),
      deleteBitmap_(nullptr) {}

IcebergSplitReader::~IcebergSplitReader() {}

void IcebergSplitReader::prepareSplit(
    std::shared_ptr<velox::common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats) {
  createReader();
  if (emptySplit_) {
    return;
  }
  auto rowType = getAdaptedRowType();

  std::shared_ptr<const IcebergConnectorSplit> icebergSplit =
      std::dynamic_pointer_cast<const IcebergConnectorSplit>(split_);
  VELOX_CHECK_NOT_NULL(icebergSplit);

  const auto& deleteFiles = icebergSplit->deleteFiles;
  std::unordered_set<int32_t> equalityFieldIds;
  for (const auto& deleteFile : deleteFiles) {
    if (deleteFile.content == FileContent::kEqualityDeletes &&
        deleteFile.recordCount > 0) {
      equalityFieldIds.insert(
          deleteFile.equalityFieldIds.begin(),
          deleteFile.equalityFieldIds.end());
    }
  }

  if (checkIfSplitIsEmpty(runtimeStats)) {
    VELOX_CHECK(emptySplit_);
    return;
  }

  createRowReader(std::move(metadataFilter), std::move(rowType));

  baseReadOffset_ = 0;
  splitOffset_ = baseRowReader_->nextRowNumber();

  // Create the positional deletes file readers. They need to be created after
  // the RowReader is created.
  positionalDeleteFileReaders_.clear();
  for (const auto& deleteFile : deleteFiles) {
    if (deleteFile.content == FileContent::kPositionalDeletes) {
      if (deleteFile.recordCount > 0) {
        positionalDeleteFileReaders_.push_back(
            std::make_unique<PositionalDeleteFileReader>(
                deleteFile,
                split_->filePath,
                fileHandleFactory_,
                connectorQueryCtx_,
                executor_,
                connectorConfig_,
                ioStats_,
                fsStats_,
                runtimeStats,
                splitOffset_,
                split_->connectorId));
      }
    }
  }
}

std::shared_ptr<const dwio::common::TypeWithId>
IcebergSplitReader::baseFileSchema() {
  VELOX_CHECK_NOT_NULL(baseReader_.get());
  return baseReader_->typeWithId();
}

uint64_t IcebergSplitReader::next(uint64_t size, VectorPtr& output) {
  Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  mutation.deletedRows = nullptr;

  if (deleteBitmap_) {
    std::memset(
        (void*)(deleteBitmap_->asMutable<int8_t>()), 0L, deleteBitmap_->size());
  }

  const auto actualSize = baseRowReader_->nextReadSize(size);

  if (actualSize == dwio::common::RowReader::kAtEnd) {
    return 0;
  }

  if (!positionalDeleteFileReaders_.empty()) {
    auto numBytes = bits::nbytes(actualSize);
    dwio::common::ensureCapacity<int8_t>(
        deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool(), false, true);

    for (auto iter = positionalDeleteFileReaders_.begin();
         iter != positionalDeleteFileReaders_.end();) {
      (*iter)->readDeletePositions(baseReadOffset_, actualSize, deleteBitmap_);

      if ((*iter)->noMoreData()) {
        iter = positionalDeleteFileReaders_.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  mutation.deletedRows = deleteBitmap_ && deleteBitmap_->size() > 0
      ? deleteBitmap_->as<uint64_t>()
      : nullptr;

  auto rowsScanned = baseRowReader_->next(actualSize, output, &mutation);

  baseReadOffset_ += rowsScanned;
  return rowsScanned;
}

bool IcebergSplitReader::filterSplit(
    dwio::common::RuntimeStatistics& runtimeStats) const {
  // TODO: Some engines like Flink may write multiple partitions in one data
  // file. Also, the Iceberg partition spec for one split may be different than
  // the other split.
  return iceberg::filterSplit(
      scanSpec_.get(),
      baseReader_.get(),
      split_->filePath,
      split_->partitionKeys,
      *partitionColumnHandles_,
      connectorConfig_->readTimestampPartitionValueAsLocalTime(
          connectorQueryCtx_->sessionProperties()));
}

} // namespace facebook::velox::connector::lakehouse::iceberg
