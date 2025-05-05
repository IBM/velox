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

#include "velox/connectors/hive_common/ConnectorDataSource.h"

#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive_common/ConnectorUtil.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/expression/FieldReference.h"
#include "velox/type/Subfield.h"

#include <fmt/ranges.h>

#include <string>
#include <unordered_map>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive_common {

class HiveTableHandle;
class HiveColumnHandle;

namespace {

bool isMember(
    const std::vector<exec::FieldReference*>& fields,
    const exec::FieldReference& field) {
  return std::find(fields.begin(), fields.end(), &field) != fields.end();
}

bool shouldEagerlyMaterialize(
    const exec::Expr& remainingFilter,
    const exec::FieldReference& field) {
  if (!remainingFilter.evaluatesArgumentsOnNonIncreasingSelection()) {
    return true;
  }
  for (auto& input : remainingFilter.inputs()) {
    if (isMember(input->distinctFields(), field) && input->hasConditionals()) {
      return true;
    }
  }
  return false;
}

} // namespace

ConnectorDataSource::ConnectorDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ConnectorConfig>& connectorConfig)
    : fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()),
      pool_(connectorQueryCtx->memoryPool()),
      connectorConfig_(connectorConfig),
      outputType_(outputType) {
  tableHandle_ = std::dynamic_pointer_cast<TableHandleBase>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tableHandle_,
      "ConnectorTableHandle must be an instance of TableHandleBase for {}",
      tableHandle->name());

  //   Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<ColumnHandleBase>(columnHandle);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);
    switch (handle->columnType()) {
      case ColumnHandleBase::ColumnType::kRegular:
        break;
      case ColumnHandleBase::ColumnType::kPartitionKey:
        partitionColumnHandles_.emplace(handle->name(), handle);
        break;
      case ColumnHandleBase::ColumnType::kSynthesized:
        infoColumns_.emplace(handle->name(), handle);
        break;
        //      case ColumnHandleBase::ColumnType::kRowIndex:
        //        specialColumns_.rowIndex = handle->name();
        //        break;
        //      case ColumnHandleBase::ColumnType::kRowId:
        //        specialColumns_.rowId = handle->name();
        //        break;
    }
  }

  std::vector<std::string> readColumnNames;
  auto readColumnTypes = outputType_->children();
  for (const auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const ColumnHandleBase*>(it->second.get());
    readColumnNames.push_back(handle->name());
    for (auto& subfield : handle->requiredSubfields()) {
      VELOX_USER_CHECK_EQ(
          getColumnName(subfield),
          handle->name(),
          "Required subfield does not match column name");
      subfields_[handle->name()].push_back(&subfield);
    }
  }

  if (connectorConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->sessionProperties())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(tableHandle_->subfieldFilters(), infoColumns_);
    checkColumnNameLowerCase(tableHandle_->remainingFilter());
  }

  for (const auto& [k, v] : tableHandle_->subfieldFilters()) {
    filters_.emplace(k.clone(), v->clone());
  }
  double sampleRate = 1;
  auto remainingFilter = extractFiltersFromRemainingFilter(
      tableHandle_->remainingFilter(),
      expressionEvaluator_,
      false,
      filters_,
      sampleRate);
  if (sampleRate != 1) {
    randomSkip_ = std::make_shared<random::RandomSkipTracker>(sampleRate);
  }

  std::vector<velox::common::Subfield> remainingFilterSubfields;
  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    auto& remainingFilterExpr = remainingFilterExprSet_->expr(0);
    folly::F14FastMap<std::string, column_index_t> columnNames;
    for (int i = 0; i < readColumnNames.size(); ++i) {
      columnNames[readColumnNames[i]] = i;
    }
    for (auto& input : remainingFilterExpr->distinctFields()) {
      auto it = columnNames.find(input->field());
      if (it != columnNames.end()) {
        if (shouldEagerlyMaterialize(*remainingFilterExpr, *input)) {
          multiReferencedFields_.push_back(it->second);
        }
        continue;
      }
      // Remaining filter may reference columns that are not used otherwise,
      // e.g. are not being projected out and are not used in range filters.
      // Make sure to add these columns to readerOutputType_.
      readColumnNames.push_back(input->field());
      readColumnTypes.push_back(input->type());
    }
    remainingFilterSubfields = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields, ", "));
    }
    for (auto& subfield : remainingFilterSubfields) {
      const auto& name = getColumnName(subfield);
      auto it = subfields_.find(name);
      if (it != subfields_.end()) {
        // Some subfields of the column are already projected out, we append the
        // remainingFilter subfield
        it->second.push_back(&subfield);
      } else if (columnNames.count(name) == 0) {
        // remainingFilter subfield's column is not projected out, we add the
        // column and append the subfield
        subfields_[name].push_back(&subfield);
      }
    }
  }

  readerOutputType_ =
      ROW(std::move(readColumnNames), std::move(readColumnTypes));

  makeScanSpec();

  if (remainingFilter) {
    metadataFilter_ = std::make_shared<velox::common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  ioStats_ = std::make_shared<io::IoStatistics>();
  fsStats_ = std::make_shared<filesystems::File::IoStats>();
}

// std::unique_ptr<SplitReader> ConnectorDataSource::createSplitReader() {
//  return SplitReader::create(
//      split_,
//      tableHandle_,
//      &partitionColumnHandles_,
//      connectorQueryCtx_,
//      connectorConfig_,
//      readerOutputType_,
//      ioStats_,
//      fsStats_,
//      fileHandleFactory_,
//      executor_,
//      scanSpec_,
//      expressionEvaluator_,
//      totalRemainingFilterTime_);
// }

// std::unique_ptr<HivePartitionFunction>
// ConnectorDataSource::setupBucketConversion() {
//  VELOX_CHECK_NE(
//      split_->bucketConversion->tableBucketCount,
//      split_->bucketConversion->partitionBucketCount);
//  VELOX_CHECK(split_->tableBucketNumber.has_value());
//  VELOX_CHECK_NOT_NULL(tableHandle_->dataColumns());
//  ++numBucketConversion_;
//  bool rebuildScanSpec = false;
//  std::vector<std::string> names;
//  std::vector<TypePtr> types;
//  std::vector<column_index_t> bucketChannels;
//  for (auto& handle : split_->bucketConversion->bucketColumnHandles) {
//    VELOX_CHECK(handle->columnType() ==
//    HiveColumnHandle::ColumnType::kRegular); if
//    (subfields_.erase(handle->name()) > 0) {
//      rebuildScanSpec = true;
//    }
//    auto index = readerOutputType_->getChildIdxIfExists(handle->name());
//    if (!index.has_value()) {
//      if (names.empty()) {
//        names = readerOutputType_->names();
//        types = readerOutputType_->children();
//      }
//      index = names.size();
//      names.push_back(handle->name());
//      types.push_back(
//          tableHandle_->dataColumns()->findChild(handle->name()));
//      rebuildScanSpec = true;
//    }
//    bucketChannels.push_back(*index);
//  }
//  if (!names.empty()) {
//    readerOutputType_ = ROW(std::move(names), std::move(types));
//  }
//  if (rebuildScanSpec) {
//    auto newScanSpec = makeScanSpec(
//        readerOutputType_,
//        subfields_,
//        filters_,
//        tableHandle_->dataColumns(),
//        partitionColumnHandles_,
//        infoColumns_,
//        specialColumns_,
//        connectorConfig_->readStatsBasedFilterReorderDisabled(
//            connectorQueryCtx_->sessionProperties()),
//        pool_);
//    newScanSpec->moveAdaptationFrom(*scanSpec_);
//    scanSpec_ = std::move(newScanSpec);
//  }
//  return std::make_unique<HivePartitionFunction>(
//      split_->bucketConversion->tableBucketCount, std::move(bucketChannels));
// }
//
// void ConnectorDataSource::setupRowIdColumn() {
//  VELOX_CHECK(split_->rowIdProperties.has_value());
//  const auto& props = *split_->rowIdProperties;
//  auto* rowId = scanSpec_->childByName(*specialColumns_.rowId);
//  VELOX_CHECK_NOT_NULL(rowId);
//  auto& rowIdType =
//      readerOutputType_->findChild(*specialColumns_.rowId)->asRow();
//  auto rowGroupId = split_->getFileName();
//  rowId->childByName(rowIdType.nameOf(1))
//      ->setConstantValue<StringView>(
//          StringView(rowGroupId), VARCHAR(),
//          connectorQueryCtx_->memoryPool());
//  rowId->childByName(rowIdType.nameOf(2))
//      ->setConstantValue<int64_t>(
//          props.metadataVersion, BIGINT(), connectorQueryCtx_->memoryPool());
//  rowId->childByName(rowIdType.nameOf(3))
//      ->setConstantValue<int64_t>(
//          props.partitionId, BIGINT(), connectorQueryCtx_->memoryPool());
//  rowId->childByName(rowIdType.nameOf(4))
//      ->setConstantValue<StringView>(
//          StringView(props.tableGuid),
//          VARCHAR(),
//          connectorQueryCtx_->memoryPool());
//}

// void ConnectorDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
//   VELOX_CHECK_NULL(
//       split_,
//       "Previous split has not been processed yet. Call next to process the
//       split.");
//   split_ = std::dynamic_pointer_cast<ConnectorSplitBase>(split);
//   VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");
//
//   VLOG(1) << "Adding split " << split_->toString();
//
//   if (splitReader_) {
//     splitReader_.reset();
//   }
//
//   if (split_->bucketConversion.has_value()) {
//     partitionFunction_ = setupBucketConversion();
//   } else {
//     partitionFunction_.reset();
//   }
//   if (specialColumns_.rowId.has_value()) {
//     setupRowIdColumn();
//   }
//
//   splitReader_ = createSplitReader();
//
//   // Split reader subclasses may need to use the reader options in
//   prepareSplit
//   // so we initialize it beforehand.
//
//   splitReader_->configureReaderOptions(randomSkip_);
//   splitReader_->prepareSplit(metadataFilter_, runtimeStats_);
//   readerOutputType_ = splitReader_->readerOutputType();
// }

// vector_size_t ConnectorDataSource::applyBucketConversion(
//     const RowVectorPtr& rowVector,
//     BufferPtr& indices) {
//   partitions_.clear();
//   partitionFunction_->partition(*rowVector, partitions_);
//   const auto bucketToKeep = *split_->tableBucketNumber;
//   const auto partitionBucketCount =
//       split_->bucketConversion->partitionBucketCount;
//   for (vector_size_t i = 0; i < rowVector->size(); ++i) {
//     VELOX_CHECK_EQ((partitions_[i] - bucketToKeep) % partitionBucketCount,
//     0);
//   }
//
//   if (remainingFilterExprSet_) {
//     for (vector_size_t i = 0; i < rowVector->size(); ++i) {
//       if (partitions_[i] != bucketToKeep) {
//         filterRows_.setValid(i, false);
//       }
//     }
//     filterRows_.updateBounds();
//     return filterRows_.countSelected();
//   }
//   vector_size_t size = 0;
//   for (vector_size_t i = 0; i < rowVector->size(); ++i) {
//     size += partitions_[i] == bucketToKeep;
//   }
//   if (size == 0) {
//     return 0;
//   }
//   indices = allocateIndices(size, pool_);
//   size = 0;
//   auto* rawIndices = indices->asMutable<vector_size_t>();
//   for (vector_size_t i = 0; i < rowVector->size(); ++i) {
//     if (partitions_[i] == bucketToKeep) {
//       rawIndices[size++] = i;
//     }
//   }
//   return size;
// }

std::optional<RowVectorPtr> ConnectorDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");

  TestValue::adjust(
      "facebook::velox::connector::hive_common::ConnectorDataSource::next", this);

  if (splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  // Bucket conversion or delta update could add extra column to reader output.
  auto needsExtraColumn = [&] {
    return output_->asUnchecked<RowVector>()->childrenSize() <
        readerOutputType_->size();
  };
  if (!output_ || needsExtraColumn()) {
    output_ = BaseVector::create(readerOutputType_, 0, pool_);
  }

  const auto rowsScanned = splitReader_->next(size, output_);
  completedRows_ += rowsScanned;
  if (rowsScanned == 0) {
    splitReader_->updateRuntimeStats(runtimeStats_);
    resetSplit();
    return nullptr;
  }

  VELOX_CHECK(
      !output_->mayHaveNulls(), "Top-level row vector cannot have nulls");
  auto rowsRemaining = output_->size();
  if (rowsRemaining == 0) {
    // no rows passed the pushed down filters.
    return getEmptyOutput();
  }

  auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);

  // In case there is a remaining filter that excludes some but not all
  // rows, collect the indices of the passing rows. If there is no filter,
  // or it passes on all rows, leave this as null and let exec::wrap skip
  // wrapping the results.
  BufferPtr remainingIndices;
  if (remainingFilterExprSet_) {
    rowsRemaining = evaluatePartitionFilter(rowVector, remainingIndices);

    if (rowsRemaining == 0) {
      return getEmptyOutput();
    }
  }

  // TODO: remove if?
  if (remainingFilterExprSet_) {
    rowsRemaining = evaluateRemainingFilter(rowVector);
    VELOX_CHECK_LE(rowsRemaining, rowsScanned);
    if (rowsRemaining == 0) {
      // No rows passed the remaining filter.
      return getEmptyOutput();
    }

    if (rowsRemaining < rowVector->size()) {
      // Some, but not all rows passed the remaining filter.
      remainingIndices = filterEvalCtx_.selectedIndices;
    }
  }

  if (outputType_->size() == 0) {
    return exec::wrap(rowsRemaining, remainingIndices, rowVector);
  }

  std::vector<VectorPtr> outputColumns;
  outputColumns.reserve(outputType_->size());
  for (int i = 0; i < outputType_->size(); ++i) {
    auto& child = rowVector->childAt(i);
    if (remainingIndices) {
      // Disable dictionary values caching in expression eval so that we
      // don't need to reallocate the result for every batch.
      child->disableMemo();
    }
    outputColumns.emplace_back(
        exec::wrapChild(rowsRemaining, remainingIndices, child));
  }

  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
}

void ConnectorDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<velox::common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.addFilter(*filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }
}

std::unordered_map<std::string, RuntimeCounter>
ConnectorDataSource::runtimeStats() {
  auto res = runtimeStats_.toMap();
  res.insert(
      {{"numPrefetch", RuntimeCounter(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeCounter(
            ioStats_->prefetch().sum(), RuntimeCounter::Unit::kBytes)},
       {"totalScanTime",
        RuntimeCounter(
            ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
       {"totalRemainingFilterTime",
        RuntimeCounter(
            totalRemainingFilterTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)},
       {"ioWaitWallNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"maxSingleIoWaitWallNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().max() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"overreadBytes",
        RuntimeCounter(
            ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)}});
  if (ioStats_->read().count() > 0) {
    res.insert({"numStorageRead", RuntimeCounter(ioStats_->read().count())});
    res.insert(
        {"storageReadBytes",
         RuntimeCounter(ioStats_->read().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ssdRead().count() > 0) {
    res.insert({"numLocalRead", RuntimeCounter(ioStats_->ssdRead().count())});
    res.insert(
        {"localReadBytes",
         RuntimeCounter(
             ioStats_->ssdRead().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ramHit().count() > 0) {
    res.insert({"numRamRead", RuntimeCounter(ioStats_->ramHit().count())});
    res.insert(
        {"ramReadBytes",
         RuntimeCounter(
             ioStats_->ramHit().sum(), RuntimeCounter::Unit::kBytes)});
  }
  //  if (numBucketConversion_ > 0) {
  //    res.insert({"numBucketConversion",
  //    RuntimeCounter(numBucketConversion_)});
  //  }

  const auto fsStats = fsStats_->stats();
  for (const auto& storageStats : fsStats) {
    res.emplace(
        storageStats.first,
        RuntimeCounter(storageStats.second.sum, storageStats.second.unit));
  }
  return res;
}

void ConnectorDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<ConnectorDataSource*>(sourceUnique.get());
  VELOX_CHECK_NOT_NULL(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  runtimeStats_.skippedSplits += source->runtimeStats_.skippedSplits;
  runtimeStats_.processedSplits += source->runtimeStats_.processedSplits;
  runtimeStats_.skippedSplitBytes += source->runtimeStats_.skippedSplitBytes;
  readerOutputType_ = std::move(source->readerOutputType_);
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  splitReader_ = std::move(source->splitReader_);
  splitReader_->setConnectorQueryCtx(connectorQueryCtx_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
  source->fsStats_->merge(*fsStats_);
  fsStats_ = std::move(source->fsStats_);

  //  numBucketConversion_ += source->numBucketConversion_;
  //  partitionFunction_ = std::move(source->partitionFunction_);
}

int64_t ConnectorDataSource::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

vector_size_t ConnectorDataSource::evaluatePartitionFilter(
    RowVectorPtr& rowVector,
    BufferPtr& remainingIndices) {}

vector_size_t ConnectorDataSource::evaluateRemainingFilter(
    RowVectorPtr& rowVector) {
  for (auto fieldIndex : multiReferencedFields_) {
    LazyVector::ensureLoadedRows(
        rowVector->childAt(fieldIndex),
        filterRows_,
        filterLazyDecoded_,
        filterLazyBaseRows_);
  }
  uint64_t filterTimeUs{0};
  vector_size_t rowsRemaining{0};
  {
    MicrosecondTimer timer(&filterTimeUs);
    expressionEvaluator_->evaluate(
        remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
    rowsRemaining = exec::processFilterResults(
        filterResult_, filterRows_, filterEvalCtx_, pool_);
  }
  totalRemainingFilterTime_.fetch_add(
      filterTimeUs * 1000, std::memory_order_relaxed);
  return rowsRemaining;
}

std::shared_ptr<velox::common::ScanSpec> ConnectorDataSource::makeScanSpec() {
  auto spec = std::make_shared<velox::common::ScanSpec>("root");
  folly::F14FastMap<std::string, std::vector<const velox::common::Subfield*>>
      filterSubfields;
  std::vector<SubfieldSpec> subfieldSpecs;
  for (auto& [subfield, _] : filters_) {
    if (auto name = subfield.toString();
        !isSynthesizedColumn(name, infoColumns_) &&
        partitionColumnHandles_.count(name) == 0) {
      //      VELOX_CHECK(!isSpecialColumn(name, specialColumns_.rowIndex));
      //      VELOX_CHECK(!isSpecialColumn(name, specialColumns_.rowId));
      filterSubfields[getColumnName(subfield)].push_back(&subfield);
    }
  }

  // Process columns that will be projected out.
  for (int i = 0; i < readerOutputType_->size(); ++i) {
    auto& name = readerOutputType_->nameOf(i);
    auto& type = readerOutputType_->childAt(i);

    // Different table formats may have different special columns. They would be
    // handled differently by corresponding connectors.
    if (isSpecialColumn(name)) {
      continue;
    }
    //    if (isSpecialColumn(name, specialColumns.rowIndex)) {
    //      VELOX_CHECK(type->isBigint());
    //      auto* fieldSpec = spec->addField(name, i);
    //      fieldSpec->setColumnType(velox::hive_common::ScanSpec::ColumnType::kRowIndex);
    //      continue;
    //    }
    //    if (isSpecialColumn(name, specialColumns.rowId)) {
    //      VELOX_CHECK(type->isRow() && type->size() == 5);
    //      auto& rowIdType = type->asRow();
    //      auto* fieldSpec = spec->addFieldRecursively(name, rowIdType, i);
    //      fieldSpec->setColumnType(velox::hive_common::ScanSpec::ColumnType::kComposite);
    //      fieldSpec->childByName(rowIdType.nameOf(0))
    //          ->setColumnType(velox::hive_common::ScanSpec::ColumnType::kRowIndex);
    //      continue;
    //    }

    auto dataColumns = tableHandle_->dataColumns();
    auto it = subfields_.find(name);
    if (it == subfields_.end()) {
      auto* fieldSpec = spec->addFieldRecursively(name, *type, i);
      processFieldSpec(dataColumns, type, *fieldSpec);
      filterSubfields.erase(name);
      continue;
    }
    for (auto* subfield : it->second) {
      subfieldSpecs.push_back({subfield, false});
    }
    it = filterSubfields.find(name);
    if (it != filterSubfields.end()) {
      for (auto* subfield : it->second) {
        subfieldSpecs.push_back({subfield, true});
      }
      filterSubfields.erase(it);
    }
    auto* fieldSpec = spec->addField(name, i);
    addSubfields(*type, subfieldSpecs, 1, pool_, *fieldSpec);
    processFieldSpec(dataColumns, type, *fieldSpec);
    subfieldSpecs.clear();
  }

  // Now process the columns that will not be projected out.
  if (!filterSubfields.empty()) {
    VELOX_CHECK_NOT_NULL(tableHandle_->dataColumns());
    for (auto& [fieldName, subfields] : filterSubfields) {
      for (auto* subfield : subfields) {
        subfieldSpecs.push_back({subfield, true});
      }
      auto& type = tableHandle_->dataColumns()->findChild(fieldName);
      auto* fieldSpec = spec->getOrCreateChild(fieldName);
      addSubfields(*type, subfieldSpecs, 1, pool_, *fieldSpec);
      processFieldSpec(tableHandle_->dataColumns(), type, *fieldSpec);
      subfieldSpecs.clear();
    }
  }

  for (auto& pair : filters_) {
    const auto name = pair.first.toString();
    // SelectiveColumnReader doesn't support constant columns with filters,
    // hence, we can't have a filter for a $path or $bucket column.
    //
    // Unfortunately, Presto happens to specify a filter for $path, $file_size,
    // $file_modified_time or $bucket column. This filter is redundant and needs
    // to be removed.
    // TODO Remove this check when Presto is fixed to not specify a filter
    // on $path and $bucket column.
    if (isSynthesizedColumn(name, infoColumns_)) {
      continue;
    }
    auto fieldSpec = spec->getOrCreateChild(pair.first);
    fieldSpec->addFilter(*pair.second);
  }

  if (connectorConfig_->readStatsBasedFilterReorderDisabled(
          connectorQueryCtx_->sessionProperties())) {
    spec->disableStatsBasedFilterReorder();
  }

  return spec;
}

bool isSpecialColumn(const std::string& name) {
  return false;
}

void ConnectorDataSource::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}
//
//ConnectorDataSource::WaveDelegateHookFunction
//    ConnectorDataSource::waveDelegateHook_;
//
//std::shared_ptr<wave::WaveDataSource> ConnectorDataSource::toWaveDataSource() {
//  VELOX_CHECK_NOT_NULL(waveDelegateHook_);
//  if (!waveDataSource_) {
//    waveDataSource_ = waveDelegateHook_(
//        tableHandle_,
//        scanSpec_,
//        readerOutputType_,
//        &partitionColumnHandles_,
//        fileHandleFactory_,
//        executor_,
//        connectorQueryCtx_,
//        connectorConfig_,
//        ioStats_,
//        remainingFilterExprSet_.get(),
//        metadataFilter_);
//  }
//  return waveDataSource_;
//}
//
////  static
//void ConnectorDataSource::registerWaveDelegateHook(
//    WaveDelegateHookFunction hook) {
//  waveDelegateHook_ = hook;
//}
//
//std::shared_ptr<wave::WaveDataSource> toWaveDataSource();

} // namespace facebook::velox::connector::hive_common
