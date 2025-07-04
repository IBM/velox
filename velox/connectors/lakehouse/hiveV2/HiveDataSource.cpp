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

#include "velox/connectors/lakehouse/hiveV2/HiveDataSource.h"

#include "velox/connectors/lakehouse/hiveV2/HiveConfig.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/experimental/wave/exec/WaveDataSource.h"
#include "velox/expression/FieldReference.h"

#include <string>
#include <unordered_map>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::lakehouse::hive {

class HiveTableHandle;
class HiveColumnHandle;

HiveDataSource::HiveDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : DataSourceBase(
          outputType,
          tableHandle,
          columnHandles,
          fileHandleFactory,
          executor,
          connectorQueryCtx,
          hiveConfig) {
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<HiveColumnHandle>(columnHandle);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);
    switch (handle->columnType()) {
      case HiveColumnHandle::ColumnType::kHiveRowIndex:
        specialColumns_.rowIndex = handle->name();
        break;
      case HiveColumnHandle::ColumnType::kHiveRowId:
        specialColumns_.rowId = handle->name();
        break;
      default:
        // Already handled by base class
        break;
    }
  }
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  if (splitReader_) {
    splitReader_.reset();
  }

  if (split_->as<HiveConnectorSplit>()->bucketConversion.has_value()) {
    partitionFunction_ = setupBucketConversion();
  } else {
    partitionFunction_.reset();
  }
  if (specialColumns_.rowId.has_value()) {
    setupRowIdColumn();
  }

  splitReader_ = std::make_unique<HiveSplitReader>(
      split_,
      tableHandle_,
      &partitionColumnHandles_,
      connectorQueryCtx_,
      ConnectorConfigBase_,
      readerOutputType_,
      ioStats_,
      fsStats_,
      fileHandleFactory_,
      executor_,
      scanSpec_);

  // Split reader subclasses may need to use the reader options in prepareSplit
  // so we initialize it beforehand.

  splitReader_->configureReaderOptions(randomSkip_);
  splitReader_->prepareSplit(metadataFilter_, runtimeStats_);
  readerOutputType_ = splitReader_->readerOutputType();
}

std::unordered_map<std::string, RuntimeCounter> HiveDataSource::runtimeStats() {
  DataSourceBase::runtimeStats();
  auto res = runtimeStats_.toMap();

  if (numBucketConversion_ > 0) {
    res.insert({"numBucketConversion", RuntimeCounter(numBucketConversion_)});
  }

  return res;
}

void HiveDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  DataSourceBase::setFromDataSource(std::move(sourceUnique));

  auto source = dynamic_cast<HiveDataSource*>(sourceUnique.get());
  VELOX_CHECK_NOT_NULL(source, "Bad DataSource type");

  numBucketConversion_ += source->numBucketConversion_;
  partitionFunction_ = std::move(source->partitionFunction_);
}


std::shared_ptr<velox::common::ScanSpec> HiveDataSource::makeScanSpec() {
  auto spec = base::DataSourceBase::makeScanSpec();
  for (int i = 0; i < readerOutputType_->size(); ++i) {
    auto& name = readerOutputType_->nameOf(i);
    auto& type = readerOutputType_->childAt(i);

    if (specialColumns_.rowIndex.has_value() &&
        name == specialColumns_.rowIndex.value()) {
      VELOX_CHECK(type->isBigint());
      auto* fieldSpec = spec->addField(name, i);
      fieldSpec->setColumnType(velox::common::ScanSpec::ColumnType::kRowIndex);
      continue;
    }
    if (specialColumns_.rowId.has_value() &&
        name == specialColumns_.rowId.value()) {
      VELOX_CHECK(type->isRow() && type->size() == 5);
      auto& rowIdType = type->asRow();
      auto* fieldSpec = scanSpec_->addFieldRecursively(name, rowIdType, i);
      fieldSpec->setColumnType(velox::common::ScanSpec::ColumnType::kComposite);
      fieldSpec->childByName(rowIdType.nameOf(0))
          ->setColumnType(velox::common::ScanSpec::ColumnType::kRowIndex);
      continue;
    }
  }

  return spec;
}

bool HiveDataSource::isSpecialColumn(const std::string& name) const {
  if ((specialColumns_.rowIndex.has_value() &&
       name == specialColumns_.rowIndex.value()) ||
      (specialColumns_.rowId.has_value() &&
       name == specialColumns_.rowId.value())) {
    return true;
  }
  return false;
}

void HiveDataSource::setupRowIdColumn() {
  VELOX_CHECK(split_->as<HiveConnectorSplit>()->rowIdProperties.has_value());
  const auto& props = *split_->as<HiveConnectorSplit>()->rowIdProperties;
  auto* rowId = scanSpec_->childByName(*specialColumns_.rowId);
  VELOX_CHECK_NOT_NULL(rowId);
  auto& rowIdType =
      readerOutputType_->findChild(*specialColumns_.rowId)->asRow();
  auto rowGroupId = split_->getFileName();
  rowId->childByName(rowIdType.nameOf(1))
      ->setConstantValue<StringView>(
          StringView(rowGroupId), VARCHAR(), connectorQueryCtx_->memoryPool());
  rowId->childByName(rowIdType.nameOf(2))
      ->setConstantValue<int64_t>(
          props.metadataVersion, BIGINT(), connectorQueryCtx_->memoryPool());
  rowId->childByName(rowIdType.nameOf(3))
      ->setConstantValue<int64_t>(
          props.partitionId, BIGINT(), connectorQueryCtx_->memoryPool());
  rowId->childByName(rowIdType.nameOf(4))
      ->setConstantValue<StringView>(
          StringView(props.tableGuid),
          VARCHAR(),
          connectorQueryCtx_->memoryPool());
}

vector_size_t HiveDataSource::evaluateRemainingPartitionFilter(
    RowVectorPtr& rowVector,
    BufferPtr& remainingIndices) {
  if (remainingFilterExprSet_) {
    if (numBucketConversion_ > 0) {
      filterRows_.resizeFill(rowVector->size());
    }
  }

  if (partitionFunction_) {
    return applyBucketConversion(rowVector, remainingIndices);
  }

  return rowVector->size();
}

std::unique_ptr<HivePartitionFunction> HiveDataSource::setupBucketConversion() {
  auto hiveSplit_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split_);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");
  VELOX_CHECK_NE(
      hiveSplit_->bucketConversion->tableBucketCount,
      hiveSplit_->bucketConversion->partitionBucketCount);
  VELOX_CHECK(hiveSplit_->tableBucketNumber.has_value());
  VELOX_CHECK_NOT_NULL(tableHandle_->dataColumns());
  ++numBucketConversion_;
  bool rebuildScanSpec = false;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<column_index_t> bucketChannels;
  for (auto& handle : hiveSplit_->bucketConversion->bucketColumnHandles) {
    VELOX_CHECK(handle->columnType() == HiveColumnHandle::ColumnType::kRegular);
    if (subfields_.erase(handle->name()) > 0) {
      rebuildScanSpec = true;
    }
    auto index = readerOutputType_->getChildIdxIfExists(handle->name());
    if (!index.has_value()) {
      if (names.empty()) {
        names = readerOutputType_->names();
        types = readerOutputType_->children();
      }
      index = names.size();
      names.push_back(handle->name());
      types.push_back(tableHandle_->dataColumns()->findChild(handle->name()));
      rebuildScanSpec = true;
    }
    bucketChannels.push_back(*index);
  }
  if (!names.empty()) {
    readerOutputType_ = ROW(std::move(names), std::move(types));
  }
  if (rebuildScanSpec) {
    auto newScanSpec = makeScanSpec();
    newScanSpec->moveAdaptationFrom(*scanSpec_);
    scanSpec_ = std::move(newScanSpec);
  }
  return std::make_unique<HivePartitionFunction>(
      hiveSplit_->bucketConversion->tableBucketCount,
      std::move(bucketChannels));
}

vector_size_t HiveDataSource::applyBucketConversion(
    const RowVectorPtr& rowVector,
    BufferPtr& indices) {
  partitions_.clear();
  partitionFunction_->partition(*rowVector, partitions_);
  const auto bucketToKeep =
      *split_->as<HiveConnectorSplit>()->tableBucketNumber;
  const auto partitionBucketCount =
      split_->as<HiveConnectorSplit>()->bucketConversion->partitionBucketCount;
  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    VELOX_CHECK_EQ((partitions_[i] - bucketToKeep) % partitionBucketCount, 0);
  }

  if (remainingFilterExprSet_) {
    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      if (partitions_[i] != bucketToKeep) {
        filterRows_.setValid(i, false);
      }
    }
    filterRows_.updateBounds();
    return filterRows_.countSelected();
  }
  vector_size_t size = 0;
  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    size += partitions_[i] == bucketToKeep;
  }
  if (size == 0) {
    return 0;
  }
  indices = allocateIndices(size, pool_);
  size = 0;
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    if (partitions_[i] == bucketToKeep) {
      rawIndices[size++] = i;
    }
  }
  return size;
}


 HiveDataSource::WaveDelegateHookFunction HiveDataSource::waveDelegateHook_;

 std::shared_ptr<wave::WaveDataSource> HiveDataSource::toWaveDataSource() {
  VELOX_CHECK_NOT_NULL(waveDelegateHook_);
  if (!waveDataSource_) {
    waveDataSource_ = waveDelegateHook_(
//        dynamic_pointer_cast<HiveTableHandle>(tableHandle_),
        tableHandle_,
        scanSpec_,
        readerOutputType_,
        &partitionColumnHandles_,
        fileHandleFactory_,
        executor_,
        connectorQueryCtx_,
        ConnectorConfigBase_,
        ioStats_,
        remainingFilterExprSet_.get(),
        metadataFilter_);
  }
  return waveDataSource_;
}

//  static
 void HiveDataSource::registerWaveDelegateHook(WaveDelegateHookFunction hook)
 {
   waveDelegateHook_ = hook;
 }

 std::shared_ptr<wave::WaveDataSource> toWaveDataSource();

} // namespace facebook::velox::connector::lakehouse::hive
