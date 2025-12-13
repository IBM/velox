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

#include "velox/connectors/hive/HiveDataSource.h"

#include <fmt/ranges.h>
#include <string>
#include <unordered_map>

#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/expression/FieldReference.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive {

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

void checkColumnHandleConsistent(
    const HiveColumnHandle& x,
    const HiveColumnHandle& y) {
  VELOX_CHECK(
      x.columnType() == y.columnType(),
      "Inconsistent column handle: {}",
      x.name());
  VELOX_CHECK_EQ(
      *x.dataType(), *y.dataType(), "Inconsistent column handle: {}", x.name());
  VELOX_CHECK_EQ(
      *x.hiveType(), *y.hiveType(), "Inconsistent column handle: {}", x.name());
}

} // namespace

void HiveDataSource::processColumnHandle(const HiveColumnHandlePtr& handle) {
  switch (handle->columnType()) {
    case HiveColumnHandle::ColumnType::kRegular:
      break;
    case HiveColumnHandle::ColumnType::kPartitionKey:
      partitionKeys_.emplace(handle->name(), handle);
      break;
    case HiveColumnHandle::ColumnType::kSynthesized:
      infoColumns_.emplace(handle->name(), handle);
      break;
    case HiveColumnHandle::ColumnType::kRowIndex:
      specialColumns_.rowIndex = handle->name();
      break;
    case HiveColumnHandle::ColumnType::kRowId:
      specialColumns_.rowId = handle->name();
      break;
  }
}

HiveDataSource::HiveDataSource(
    const RowTypePtr& outputType,
    const connector::ConnectorTableHandlePtr& tableHandle,
    const connector::ColumnHandleMap& assignments,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* ioExecutor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    bool pushdownCasts)
    : assignments_(assignments),
      fileHandleFactory_(fileHandleFactory),
      ioExecutor_(ioExecutor),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      pool_(connectorQueryCtx->memoryPool()),
      outputType_(outputType),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()),
      pushdownCasts_(pushdownCasts) {
  hiveTableHandle_ =
      std::dynamic_pointer_cast<const HiveTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      hiveTableHandle_, "TableHandle must be an instance of HiveTableHandle");

  folly::F14FastMap<std::string_view, const HiveColumnHandle*> columnHandles;
  // Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : assignments) {
    auto handle =
        std::dynamic_pointer_cast<const HiveColumnHandle>(columnHandle);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);
    const auto [it, unique] =
        columnHandles.emplace(handle->name(), handle.get());
    if (!unique) {
      // This should not happen normally, but there is some bug in Presto DELETE
      // queries that sometimes we do get duplicate assignments for partitioning
      // columns.
      checkColumnHandleConsistent(*handle, *it->second);
      VELOX_CHECK(
          handle->columnType() == HiveColumnHandle::ColumnType::kPartitionKey,
          "Cannot map from same table column to different outputs in table scan; a project node should be used instead: {}",
          handle->name());
      continue;
    }
    processColumnHandle(handle);
  }
  for (auto& handle : hiveTableHandle_->filterColumnHandles()) {
    auto it = columnHandles.find(handle->name());
    if (it != columnHandles.end()) {
      checkColumnHandleConsistent(*handle, *it->second);
      continue;
    }
    processColumnHandle(handle);
  }

  std::vector<std::string> readColumnNames;
  std::vector<TypePtr> readColumnTypes;
  std::vector<std::string> readColumnNamesWithoutUpcasts;
  std::vector<TypePtr> readColumnTypesWithoutUpcasts;

  // outputType_ contains the upcast columns if pushdownCasts_ is true.
  for (int i = 0; i < outputType_->size(); ++i) {
    auto columnName = outputType_->nameOf(i); // e.g. order_id_21_upcast
    auto& columnType = outputType_->childAt(i);

    auto originalColumnName = columnName;
    if (pushdownCasts_ && columnName.ends_with("_upcast")) {
      originalColumnName =
          columnName.substr(0, columnName.size() - strlen("_upcast"));
    }

    // Get the ColumnHandle name. This is the name without aliasing. e.g.
    // originalColumnName="order_id_21", and columnHandleName="order_id"
    auto it = assignments_.find(originalColumnName);
    VELOX_CHECK(
        it != assignments_.end(),
        "ColumnHandle is missing for output column: {}",
        columnName);
    auto* handle = static_cast<const HiveColumnHandle*>(it->second.get());
    auto columnHandleName = handle->name();

    if (!pushdownCasts_ || !columnName.ends_with("_upcast")) {
      readColumnNamesWithoutUpcasts.push_back(columnHandleName);
      readColumnTypesWithoutUpcasts.push_back(columnType);
    }
    readColumnNames.push_back(columnHandleName);
    readColumnTypes.push_back(columnType);

    for (auto& subfield : handle->requiredSubfields()) {
      VELOX_USER_CHECK_EQ(
          getColumnName(subfield),
          handle->name(),
          "Required subfield does not match column name");
      subfields_[columnHandleName].push_back(&subfield);
    }
    columnPostProcessors_.push_back(handle->postProcessor());
  }

  if (hiveConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->sessionProperties())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(hiveTableHandle_->subfieldFilters(), infoColumns_);
    checkColumnNameLowerCase(hiveTableHandle_->remainingFilter());
  }

  for (const auto& [k, v] : hiveTableHandle_->subfieldFilters()) {
    filters_.emplace(k.clone(), v);
  }
  double sampleRate = hiveTableHandle_->sampleRate();
  auto remainingFilter = extractFiltersFromRemainingFilter(
      hiveTableHandle_->remainingFilter(),
      expressionEvaluator_,
      filters_,
      sampleRate);
  if (sampleRate != 1) {
    randomSkip_ = std::make_shared<random::RandomSkipTracker>(sampleRate);
  }

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
      // Make sure to add these columns to readerOutputTypeWithoutUpcasts_.
      readColumnNames.push_back(input->field());
      readColumnTypes.push_back(input->type());
    }
    remainingFilterSubfields_ = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields_, ", "));
    }
    for (auto& subfield : remainingFilterSubfields_) {
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
  // NO upcast columns
  readerOutputTypeWithoutUpcasts_ =
      ROW(std::move(readColumnNamesWithoutUpcasts),
          std::move(readColumnTypesWithoutUpcasts));
  scanSpec_ = makeScanSpec(
      readerOutputTypeWithoutUpcasts_,
      subfields_,
      filters_,
      hiveTableHandle_->dataColumns(),
      partitionKeys_,
      infoColumns_,
      specialColumns_,
      hiveConfig_->readStatsBasedFilterReorderDisabled(
          connectorQueryCtx_->sessionProperties()),
      pool_);
  if (remainingFilter) {
    metadataFilter_ = std::make_shared<common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  ioStats_ = std::make_shared<io::IoStatistics>();
  fsStats_ = std::make_shared<filesystems::File::IoStats>();
}

std::unique_ptr<SplitReader> HiveDataSource::createSplitReader() {
  return SplitReader::create(
      split_,
      hiveTableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      hiveConfig_,
      readerOutputTypeWithoutUpcasts_,
      ioStats_,
      fsStats_,
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_,
      expressionEvaluator_,
      totalRemainingFilterTime_);
}

std::vector<column_index_t> HiveDataSource::setupBucketConversion() {
  VELOX_CHECK_NE(
      split_->bucketConversion->tableBucketCount,
      split_->bucketConversion->partitionBucketCount);
  VELOX_CHECK(split_->tableBucketNumber.has_value());
  VELOX_CHECK_NOT_NULL(hiveTableHandle_->dataColumns());
  ++numBucketConversion_;
  bool rebuildScanSpec = false;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<column_index_t> bucketChannels;
  for (auto& handle : split_->bucketConversion->bucketColumnHandles) {
    VELOX_CHECK(handle->columnType() == HiveColumnHandle::ColumnType::kRegular);
    if (subfields_.erase(handle->name()) > 0) {
      rebuildScanSpec = true;
    }
    auto index =
        readerOutputTypeWithoutUpcasts_->getChildIdxIfExists(handle->name());
    if (!index.has_value()) {
      if (names.empty()) {
        names = readerOutputTypeWithoutUpcasts_->names();
        types = readerOutputTypeWithoutUpcasts_->children();
      }
      index = names.size();
      names.push_back(handle->name());
      types.push_back(
          hiveTableHandle_->dataColumns()->findChild(handle->name()));
      rebuildScanSpec = true;
    }
    bucketChannels.push_back(*index);
  }
  if (!names.empty()) {
    readerOutputTypeWithoutUpcasts_ = ROW(std::move(names), std::move(types));
  }
  if (rebuildScanSpec) {
    auto newScanSpec = makeScanSpec(
        readerOutputTypeWithoutUpcasts_,
        subfields_,
        filters_,
        hiveTableHandle_->dataColumns(),
        partitionKeys_,
        infoColumns_,
        specialColumns_,
        hiveConfig_->readStatsBasedFilterReorderDisabled(
            connectorQueryCtx_->sessionProperties()),
        pool_);
    newScanSpec->moveAdaptationFrom(*scanSpec_);
    scanSpec_ = std::move(newScanSpec);
  }
  return bucketChannels;
}

void HiveDataSource::setupRowIdColumn() {
  VELOX_CHECK(split_->rowIdProperties.has_value());
  const auto& props = *split_->rowIdProperties;
  auto* rowId = scanSpec_->childByName(*specialColumns_.rowId);
  VELOX_CHECK_NOT_NULL(rowId);
  auto& rowIdType =
      readerOutputTypeWithoutUpcasts_->findChild(*specialColumns_.rowId)
          ->asRow();
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

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split_,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");

  if (splitReader_) {
    splitReader_.reset();
  }

  std::vector<column_index_t> bucketChannels;
  if (split_->bucketConversion.has_value()) {
    bucketChannels = setupBucketConversion();
  }
  if (specialColumns_.rowId.has_value()) {
    setupRowIdColumn();
  }

  splitReader_ = createSplitReader();

  if (!bucketChannels.empty()) {
    splitReader_->setBucketConversion(std::move(bucketChannels));
  }
  // Split reader subclasses may need to use the reader options in prepareSplit
  // so we initialize it beforehand.
  splitReader_->configureReaderOptions(randomSkip_);
  splitReader_->prepareSplit(metadataFilter_, runtimeStats_);
  readerOutputTypeWithoutUpcasts_ = splitReader_->readerOutputType();
}

std::optional<RowVectorPtr> HiveDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");

  TestValue::adjust(
      "facebook::velox::connector::hive::HiveDataSource::next", this);

  if (splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  // Bucket conversion or delta update could add extra column to reader output.
  auto needsExtraColumn = [&] {
    return outputWithoutUpcasts_->asUnchecked<RowVector>()->childrenSize() <
        readerOutputTypeWithoutUpcasts_->size();
  };
  if (!outputWithoutUpcasts_ || needsExtraColumn()) {
    outputWithoutUpcasts_ =
        BaseVector::create(readerOutputTypeWithoutUpcasts_, 0, pool_);
  }

  // Read only the real columns, not the upcast columns.
  const auto rowsScanned = splitReader_->next(size, outputWithoutUpcasts_);
  completedRows_ += rowsScanned;
  if (rowsScanned == 0) {
    splitReader_->updateRuntimeStats(runtimeStats_);
    resetSplit();
    return nullptr;
  }

  VELOX_CHECK(
      !outputWithoutUpcasts_->mayHaveNulls(),
      "Top-level row vector cannot have nulls");
  auto rowsRemaining = outputWithoutUpcasts_->size();
  if (rowsRemaining == 0) {
    // no rows passed the pushed down filters.
    return getEmptyOutput();
  }

  auto rowVector = std::dynamic_pointer_cast<RowVector>(outputWithoutUpcasts_);

  // In case there is a remaining filter that excludes some but not all
  // rows, collect the indices of the passing rows. If there is no filter,
  // or it passes on all rows, leave this as null and let exec::wrap skip
  // wrapping the results.
  BufferPtr remainingIndices;
  filterRows_.resize(rowVector->size());

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
    std::shared_ptr<BaseVector> child;
    // find the upcast columns and add them to outputWithoutUpcasts_
    const auto& columnName = outputType_->nameOf(i);
    // outputType_ includes the upcast columns,
    const auto& columnType = outputType_->childAt(i);

    if (columnName.ends_with("_upcast")) {
      auto originalOutputName =
          columnName.substr(0, columnName.size() - strlen("_upcast"));
      auto columnHandleIt = assignments_.find(originalOutputName);
      VELOX_CHECK(
          columnHandleIt != assignments_.end(),
          "Cannot find column handle for upcast column: {} original: {}",
          columnName,
          originalOutputName);
      auto columnHandleName =
          static_cast<const HiveColumnHandle*>(columnHandleIt->second.get())
              ->name();

      //  rowVector does not have the upcast columns.
      auto index = readerOutputTypeWithoutUpcasts_->getChildIdxIfExists(
          columnHandleName);
      VELOX_CHECK(index.has_value());
      auto originalColumn = rowVector->childAt(*index);

      child = BaseVector::create(columnType, originalColumn->size(), pool_);
      child->copy(originalColumn.get(), 0, 0, originalColumn->size());
    } else {
      auto columnHandleIt = assignments_.find(columnName);
      VELOX_CHECK(
          columnHandleIt != assignments_.end(),
          "Cannot find column handle for upcast column: {} original: {}",
          columnName,
          columnName);
      auto columnHandleName =
          static_cast<const HiveColumnHandle*>(columnHandleIt->second.get())
              ->name();
      auto index =
          readerOutputTypeWithoutUpcasts_->getChildIdxIfExists(columnHandleName);
      VELOX_CHECK(index.has_value());
      child = rowVector->childAt(*index);
    }

    auto column = exec::wrapChild(rowsRemaining, remainingIndices, child);
    if (columnPostProcessors_[i]) {
      columnPostProcessors_[i](column);
    }
    outputColumns.push_back(std::move(column));
  }

  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
}

void HiveDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.setFilter(filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }
}

std::unordered_map<std::string, RuntimeMetric>
HiveDataSource::getRuntimeStats() {
  auto res = runtimeStats_.toRuntimeMetricMap();
  res.insert(
      {{"numPrefetch", RuntimeMetric(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeMetric(
            ioStats_->prefetch().sum(),
            ioStats_->prefetch().count(),
            ioStats_->prefetch().min(),
            ioStats_->prefetch().max(),
            RuntimeCounter::Unit::kBytes)},
       {"totalScanTime",
        RuntimeMetric(ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
       {Connector::kTotalRemainingFilterTime,
        RuntimeMetric(
            totalRemainingFilterTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)},
       {"ioWaitWallNanos",
        RuntimeMetric(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            ioStats_->queryThreadIoLatency().count(),
            ioStats_->queryThreadIoLatency().min() * 1000,
            ioStats_->queryThreadIoLatency().max() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"overreadBytes",
        RuntimeMetric(
            ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)}});
  if (ioStats_->read().count() > 0) {
    res.insert(
        {"storageReadBytes",
         RuntimeMetric(
             ioStats_->read().sum(),
             ioStats_->read().count(),
             ioStats_->read().min(),
             ioStats_->read().max(),
             RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ssdRead().count() > 0) {
    res.insert({"numLocalRead", RuntimeMetric(ioStats_->ssdRead().count())});
    res.insert(
        {"localReadBytes",
         RuntimeMetric(
             ioStats_->ssdRead().sum(),
             ioStats_->ssdRead().count(),
             ioStats_->ssdRead().min(),
             ioStats_->ssdRead().max(),
             RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ramHit().count() > 0) {
    res.insert({"numRamRead", RuntimeMetric(ioStats_->ramHit().count())});
    res.insert(
        {"ramReadBytes",
         RuntimeMetric(
             ioStats_->ramHit().sum(),
             ioStats_->ramHit().count(),
             ioStats_->ramHit().min(),
             ioStats_->ramHit().max(),
             RuntimeCounter::Unit::kBytes)});
  }
  if (numBucketConversion_ > 0) {
    res.insert({"numBucketConversion", RuntimeMetric(numBucketConversion_)});
  }

  const auto fsStats = fsStats_->stats();
  for (const auto& storageStats : fsStats) {
    res.emplace(storageStats.first, storageStats.second);
  }
  return res;
}

void HiveDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<HiveDataSource*>(sourceUnique.get());
  VELOX_CHECK_NOT_NULL(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  runtimeStats_.skippedSplits += source->runtimeStats_.skippedSplits;
  runtimeStats_.processedSplits += source->runtimeStats_.processedSplits;
  runtimeStats_.skippedSplitBytes += source->runtimeStats_.skippedSplitBytes;
  readerOutputTypeWithoutUpcasts_ =
      std::move(source->readerOutputTypeWithoutUpcasts_);
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  metadataFilter_ = std::move(source->metadataFilter_);
  splitReader_ = std::move(source->splitReader_);
  splitReader_->setConnectorQueryCtx(connectorQueryCtx_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
  source->fsStats_->merge(*fsStats_);
  fsStats_ = std::move(source->fsStats_);

  numBucketConversion_ += source->numBucketConversion_;
}

int64_t HiveDataSource::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

vector_size_t HiveDataSource::evaluateRemainingFilter(RowVectorPtr& rowVector) {
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

void HiveDataSource::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}

HiveDataSource::WaveDelegateHookFunction HiveDataSource::waveDelegateHook_;

std::shared_ptr<wave::WaveDataSource> HiveDataSource::toWaveDataSource() {
  VELOX_CHECK_NOT_NULL(waveDelegateHook_);
  if (!waveDataSource_) {
    waveDataSource_ = waveDelegateHook_(
        hiveTableHandle_,
        scanSpec_,
        readerOutputTypeWithoutUpcasts_,
        &partitionKeys_,
        fileHandleFactory_,
        ioExecutor_,
        connectorQueryCtx_,
        hiveConfig_,
        ioStats_,
        remainingFilterExprSet_.get(),
        metadataFilter_);
  }
  return waveDataSource_;
}

//  static
void HiveDataSource::registerWaveDelegateHook(WaveDelegateHookFunction hook) {
  waveDelegateHook_ = hook;
}
std::shared_ptr<wave::WaveDataSource> toWaveDataSource();

} // namespace facebook::velox::connector::hive
