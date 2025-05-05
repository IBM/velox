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

#include "velox/connectors/hive_common/SplitReader.h"

#include "velox/common/caching/CacheTTLController.h"
#include "velox/connectors/hive_common/ConnectorConfig.h"
#include "velox/connectors/hive_common/ConnectorSplitBase.h"
#include "velox/connectors/hive_common/ConnectorUtil.h"
//#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/ReaderFactory.h"

using namespace facebook::velox::common;

namespace facebook::velox::connector::hive_common {

SplitReader::SplitReader(
    const std::shared_ptr<const ConnectorSplitBase>& split,
    const std::shared_ptr<const TableHandleBase>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandleBase>>*
        partitionColumnHandles,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const ConnectorConfig>& connectorConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const std::shared_ptr<filesystems::File::IoStats>& fsStats,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<ScanSpec>& scanSpec)
    : split_(split),
      tableHandle_(tableHandle),
      partitionColumnHandles_(partitionColumnHandles),
      connectorQueryCtx_(connectorQueryCtx),
      connectorConfig_(connectorConfig),
      readerOutputType_(readerOutputType),
      ioStats_(ioStats),
      fsStats_(fsStats),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      pool_(connectorQueryCtx->memoryPool()),
      scanSpec_(scanSpec),
      baseReaderOpts_(connectorQueryCtx->memoryPool()),
      emptySplit_(false) {}

void SplitReader::configureReaderOptions(
    std::shared_ptr<velox::random::RandomSkipTracker> randomSkip) {
  ::facebook::velox::connector::hive_common::configureReaderOptions(
      connectorConfig_,
      connectorQueryCtx_,
      tableHandle_->dataColumns(),
      split_,
      tableHandle_->tableParameters(),
      baseReaderOpts_);
  baseReaderOpts_.setRandomSkip(std::move(randomSkip));
  baseReaderOpts_.setScanSpec(scanSpec_);
  baseReaderOpts_.setFileFormat(split_->fileFormat);
}

void SplitReader::prepareSplit(
    std::shared_ptr<velox::common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats) {
  createReader();
  if (emptySplit_) {
    return;
  }
  auto rowType = getAdaptedRowType();

  if (checkIfSplitIsEmpty(runtimeStats)) {
    VELOX_CHECK(emptySplit_);
    return;
  }

  createRowReader(std::move(metadataFilter), std::move(rowType));
}

uint64_t SplitReader::next(uint64_t size, VectorPtr& output) {
  if (!baseReaderOpts_.randomSkip()) {
    return baseRowReader_->next(size, output);
  }
  dwio::common::Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  return baseRowReader_->next(size, output, &mutation);
}

void SplitReader::resetFilterCaches() {
  if (baseRowReader_) {
    baseRowReader_->resetFilterCaches();
  }
}

bool SplitReader::emptySplit() const {
  return emptySplit_;
}

void SplitReader::resetSplit() {
  split_.reset();
}

int64_t SplitReader::estimatedRowSize() const {
  if (!baseRowReader_) {
    return DataSource::kUnknownRowSize;
  }

  const auto size = baseRowReader_->estimatedRowSize();
  return size.value_or(DataSource::kUnknownRowSize);
}

void SplitReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  if (baseRowReader_) {
    baseRowReader_->updateRuntimeStats(stats);
  }
}

bool SplitReader::allPrefetchIssued() const {
  return baseRowReader_ && baseRowReader_->allPrefetchIssued();
}

void SplitReader::setConnectorQueryCtx(
    const ConnectorQueryCtx* connectorQueryCtx) {
  connectorQueryCtx_ = connectorQueryCtx;
}

void SplitReader::createReader() {
  VELOX_CHECK_NE(
      baseReaderOpts_.fileFormat(), dwio::common::FileFormat::UNKNOWN);

  FileHandleCachedPtr fileHandleCachePtr;
  //  try {
  fileHandleCachePtr = fileHandleFactory_->generate(
      split_->filePath,
      split_->properties.has_value() ? &*split_->properties : nullptr,
      fsStats_ ? fsStats_.get() : nullptr);
  VELOX_CHECK_NOT_NULL(fileHandleCachePtr.get());

  // Here we keep adding new entries to CacheTTLController when new fileHandles
  // are generated, if CacheTTLController was created. Creator of
  // CacheTTLController needs to make sure a size control strategy was available
  // such as removing aged out entries.
  if (auto* cacheTTLController = cache::CacheTTLController::getInstance()) {
    cacheTTLController->addOpenFileInfo(fileHandleCachePtr->uuid.id());
  }
  auto baseFileInput = createBufferedInput(
      *fileHandleCachePtr,
      baseReaderOpts_,
      connectorQueryCtx_,
      ioStats_,
      fsStats_,
      executor_);

  baseReader_ = dwio::common::getReaderFactory(baseReaderOpts_.fileFormat())
                    ->createReader(std::move(baseFileInput), baseReaderOpts_);
}

RowTypePtr SplitReader::getAdaptedRowType() const {
  auto& fileType = baseReader_->rowType();
  auto columnTypes = adaptColumns(fileType, baseReaderOpts_.fileSchema());
  auto columnNames = fileType->names();
  return ROW(std::move(columnNames), std::move(columnTypes));
}

bool SplitReader::checkIfSplitIsEmpty(
    dwio::common::RuntimeStatistics& runtimeStats) {
  // emptySplit_ may already be set if the data file is not found. In this
  // case we don't need to test further.
  if (emptySplit_) {
    return true;
  }

  if (!baseReader_ || baseReader_->numberOfRows() == 0 ||
      !filterSplit(runtimeStats)) {
    emptySplit_ = true;
    ++runtimeStats.skippedSplits;
    runtimeStats.skippedSplitBytes += split_->length;
  } else {
    ++runtimeStats.processedSplits;
  }

  return emptySplit_;
}

void SplitReader::createRowReader(
    std::shared_ptr<velox::common::MetadataFilter> metadataFilter,
    RowTypePtr rowType) {
  VELOX_CHECK_NULL(baseRowReader_);
  configureRowReaderOptions(
      tableHandle_->tableParameters(),
      scanSpec_,
      std::move(metadataFilter),
      std::move(rowType),
      split_,
      connectorConfig_,
      connectorQueryCtx_->sessionProperties(),
      baseRowReaderOpts_);
  baseRowReader_ = baseReader_->createRowReader(baseRowReaderOpts_);
}

std::vector<TypePtr> SplitReader::adaptColumns(
    const RowTypePtr& fileType,
    const std::shared_ptr<const velox::RowType>& tableSchema) const {
  // Keep track of schema types for columns in file, used by ColumnSelector.
  std::vector<TypePtr> columnTypes = fileType->children();

  auto& childrenSpecs = scanSpec_->children();
  for (size_t i = 0; i < childrenSpecs.size(); ++i) {
    auto* childSpec = childrenSpecs[i].get();
    const std::string& fieldName = childSpec->fieldName();

    // Partition keys will be handled by the corresponding connector's
    // SplitReaders.
    if (auto iter = split_->infoColumns.find(fieldName);
        iter != split_->infoColumns.end()) {
      auto infoColumnType =
          readerOutputType_->childAt(readerOutputType_->getChildIdx(fieldName));
      auto constant = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          newConstantFromString,
          infoColumnType->kind(),
          infoColumnType,
          iter->second,
          1,
          connectorQueryCtx_->memoryPool(),
          connectorQueryCtx_->sessionTimezone(),
          connectorConfig_->readTimestampPartitionValueAsLocalTime(
              connectorQueryCtx_->sessionProperties()));
      childSpec->setConstantValue(constant);
    } else if (
        childSpec->columnType() ==
        velox::common::ScanSpec::ColumnType::kRegular) {
      auto fileTypeIdx = fileType->getChildIdxIfExists(fieldName);
      if (!fileTypeIdx.has_value()) {
        // Column is missing. Most likely due to schema evolution.
        VELOX_CHECK(tableSchema, "Unable to resolve column '{}'", fieldName);
        childSpec->setConstantValue(
            BaseVector::createNullConstant(
                tableSchema->findChild(fieldName),
                1,
                connectorQueryCtx_->memoryPool()));
      } else {
        // Column no longer missing, reset constant value set on the spec.
        childSpec->setConstantValue(nullptr);
        auto outputTypeIdx = readerOutputType_->getChildIdxIfExists(fieldName);
        if (outputTypeIdx.has_value()) {
          auto& outputType = readerOutputType_->childAt(*outputTypeIdx);
          auto& columnType = columnTypes[*fileTypeIdx];
          if (childSpec->isFlatMapAsStruct()) {
            // Flat map column read as struct.  Leave the schema type as MAP.
            VELOX_CHECK(outputType->isRow() && columnType->isMap());
          } else {
            // We know the fieldName exists in the file, make the type at that
            // position match what we expect in the output.
            columnType = outputType;
          }
        }
      }
    }
  }

  scanSpec_->resetCachedValues(false);

  return columnTypes;
}

std::string SplitReader::toStringBase(const std::string& className) const {
  std::string partitionKeys;
  std::for_each(
      partitionColumnHandles_->begin(),
      partitionColumnHandles_->end(),
      [&](const auto& column) {
        partitionKeys += " " + column.second->toString();
      });
  return fmt::format(
      "{}: split_{} scanSpec_{} readerOutputType_{} partitionColumnHandles_{} reader{} rowReader{}",
      className,
      split_->toString(),
      scanSpec_->toString(),
      readerOutputType_->toString(),
      partitionKeys,
      static_cast<const void*>(baseReader_.get()),
      static_cast<const void*>(baseRowReader_.get()));
}

void SplitReader::setPartitionValue(
    velox::common::ScanSpec* spec,
    const std::string& partitionKey,
    const std::optional<std::string>& value) const {
  auto it = partitionColumnHandles_->find(partitionKey);
  VELOX_CHECK(
      it != partitionColumnHandles_->end(),
      "ColumnHandle is missing for partition key {}",
      partitionKey);
  auto type = it->second->dataType();
  auto constant = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      newConstantFromString,
      type->kind(),
      type,
      value,
      1,
      connectorQueryCtx_->memoryPool(),
      connectorQueryCtx_->sessionTimezone(),
      connectorConfig_->readTimestampPartitionValueAsLocalTime(
          connectorQueryCtx_->sessionProperties()),
      it->second->isPartitionDateValueDaysSinceEpoch());
  spec->setConstantValue(constant);
}

} // namespace facebook::velox::connector::hive_common
