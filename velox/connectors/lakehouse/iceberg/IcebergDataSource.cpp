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

#include "IcebergDataSource.h"

//#include "velox/connectors/iceberg/IcebergConfig.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/experimental/wave/exec/WaveDataSource.h"
#include "velox/expression/FieldReference.h"

#include <string>
#include <unordered_map>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::lakehouse::iceberg  {

class IcebergTableHandle;
class IcebergColumnHandle;

IcebergDataSource::IcebergDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    base::FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<IcebergConfig>& icebergConfig)
    : DataSourceBase(
          outputType,
          tableHandle,
          columnHandles,
          fileHandleFactory,
          executor,
          connectorQueryCtx,
          icebergConfig) {}

void IcebergDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<IcebergConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  if (splitReader_) {
    splitReader_.reset();
  }

  splitReader_ = std::make_unique<IcebergSplitReader>(
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
//
// std::optional<RowVectorPtr> IcebergDataSource::next(
//    uint64_t size,
//    velox::ContinueFuture& /*future*/) {
//  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
//  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");
//
//  TestValue::adjust(
//      "facebook::velox::connector::lakehouse::iceberg ::IcebergDataSource::next", this);
//
//  if (splitReader_->emptySplit()) {
//    resetSplit();
//    return nullptr;
//  }
//
//  // Bucket conversion or delta update could add extra column to reader
//  output. auto needsExtraColumn = [&] {
//    return output_->asUnchecked<RowVector>()->childrenSize() <
//        readerOutputType_->size();
//  };
//  if (!output_ || needsExtraColumn()) {
//    output_ = BaseVector::create(readerOutputType_, 0, pool_);
//  }
//
//  const auto rowsScanned = splitReader_->next(size, output_);
//  completedRows_ += rowsScanned;
//  if (rowsScanned == 0) {
//    splitReader_->updateRuntimeStats(runtimeStats_);
//    resetSplit();
//    return nullptr;
//  }
//
//  VELOX_CHECK(
//      !output_->mayHaveNulls(), "Top-level row vector cannot have nulls");
//  auto rowsRemaining = output_->size();
//  if (rowsRemaining == 0) {
//    // no rows passed the pushed down filters.
//    return getEmptyOutput();
//  }
//
//  auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);
//
//  // In case there is a remaining filter that excludes some but not all
//  // rows, collect the indices of the passing rows. If there is no filter,
//  // or it passes on all rows, leave this as null and let exec::wrap skip
//  // wrapping the results.
//  BufferPtr remainingIndices;
//  if (remainingFilterExprSet_) {
//    if (numBucketConversion_ > 0) {
//      filterRows_.resizeFill(rowVector->size());
//    } else {
//      filterRows_.resize(rowVector->size());
//    }
//  }
//  if (partitionFunction_) {
//    rowsRemaining = applyBucketConversion(rowVector, remainingIndices);
//    if (rowsRemaining == 0) {
//      return getEmptyOutput();
//    }
//  }
//
//  if (remainingFilterExprSet_) {
//    rowsRemaining = evaluateRemainingFilter(rowVector);
//    VELOX_CHECK_LE(rowsRemaining, rowsScanned);
//    if (rowsRemaining == 0) {
//      // No rows passed the remaining filter.
//      return getEmptyOutput();
//    }
//
//    if (rowsRemaining < rowVector->size()) {
//      // Some, but not all rows passed the remaining filter.
//      remainingIndices = filterEvalCtx_.selectedIndices;
//    }
//  }
//
//  if (outputType_->size() == 0) {
//    return exec::wrap(rowsRemaining, remainingIndices, rowVector);
//  }
//
//  std::vector<VectorPtr> outputColumns;
//  outputColumns.reserve(outputType_->size());
//  for (int i = 0; i < outputType_->size(); ++i) {
//    auto& child = rowVector->childAt(i);
//    if (remainingIndices) {
//      // Disable dictionary values caching in expression eval so that we
//      // don't need to reallocate the result for every batch.
//      child->disableMemo();
//    }
//    outputColumns.emplace_back(
//        exec::wrapChild(rowsRemaining, remainingIndices, child));
//  }
//
//  return std::make_shared<RowVector>(
//      pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
//}

//
// void IcebergDataSource::addDynamicFilter(
//    column_index_t outputChannel,
//    const std::shared_ptr<velox::common::Filter>& filter) {
//  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
//  fieldSpec.addFilter(*filter);
//  scanSpec_->resetCachedValues(true);
//  if (splitReader_) {
//    splitReader_->resetFilterCaches();
//  }
//}

// int64_t IcebergDataSource::estimatedRowSize() {
//   if (!splitReader_) {
//     return kUnknownRowSize;
//   }
//   return splitReader_->estimatedRowSize();
// }

std::shared_ptr<velox::common::ScanSpec> IcebergDataSource::makeScanSpec() {
  auto spec = base::DataSourceBase::makeScanSpec();
  for (int i = 0; i < readerOutputType_->size(); ++i) {
    auto& name = readerOutputType_->nameOf(i);
    auto& type = readerOutputType_->childAt(i);

    if (specialColumns_.rowIndex.has_value() &&
        name == specialColumns_.rowIndex.value()) {
      VELOX_CHECK(type->isBigint());
      auto* fieldSpec = spec->addField(name, i);
      fieldSpec->setColumnType(velox::common::ScanSpec::ColumnType::kHiveRowIndex);
      continue;
    }
    if (specialColumns_.rowId.has_value() &&
        name == specialColumns_.rowId.value()) {
      VELOX_CHECK(type->isRow() && type->size() == 5);
      auto& rowIdType = type->asRow();
      auto* fieldSpec = scanSpec_->addFieldRecursively(name, rowIdType, i);
      fieldSpec->setColumnType(velox::common::ScanSpec::ColumnType::kComposite);
      fieldSpec->childByName(rowIdType.nameOf(0))
          ->setColumnType(velox::common::ScanSpec::ColumnType::kHiveRowIndex);
      continue;
    }
  }

  return spec;
}

bool IcebergDataSource::isSpecialColumn(const std::string& name) const {
  // TODO: is_deleted, etc.
  return false;
}

vector_size_t IcebergDataSource::evaluateRemainingPartitionFilter(
    RowVectorPtr& rowVector,
    BufferPtr& remainingIndices) {
  // If there are filter functions on the partition columns, evaluate them here

  return rowVector->size();
}

} // namespace facebook::velox::connector::lakehouse::iceberg
