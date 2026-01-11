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

#include "velox/connectors/lakehouse/iceberg/IcebergDataSource.h"

#include "velox/dwio/common/ReaderFactory.h"

#include <string>
#include <unordered_map>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::lakehouse::iceberg  {

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

class IcebergTableHandle;
class IcebergColumnHandle;

IcebergDataSource::IcebergDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const connector::ColumnHandleMap& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ConnectorConfigBase>& connectorConfig)
    : DataSourceBase(
          outputType,
          tableHandle,
          columnHandles,
          fileHandleFactory,
          executor,
          connectorQueryCtx,
          connectorConfig) {
  tableHandle_ = std::dynamic_pointer_cast<const TableHandleBase>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tableHandle_,
      "ConnectorTableHandle must be an instance of TableHandleBase for {}",
      tableHandle->name());

  //   Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle = std::dynamic_pointer_cast<const ColumnHandleBase>(columnHandle);
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
      default:
        break;
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

  scanSpec_ = makeScanSpec();

  if (remainingFilter) {
    metadataFilter_ = std::make_shared<velox::common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  ioStats_ = std::make_shared<io::IoStatistics>();
  fsStats_ = std::make_shared<filesystems::File::IoStats>();
}

std::optional<RowVectorPtr> IcebergDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");

  TestValue::adjust(
      "facebook::velox::connector::lakehouse::common::DataSourceBase::next",
      this);

  if (splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  if (!output_) {
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
  if (hasRemainingPartitionFilter()) {
    rowsRemaining =
        evaluateRemainingPartitionFilter(rowVector, remainingIndices);

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

void IcebergDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split_,
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
      connectorConfig_,
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

std::shared_ptr<velox::common::ScanSpec> IcebergDataSource::makeScanSpec() {
  auto spec = std::make_shared<velox::common::ScanSpec>("root");
  folly::F14FastMap<std::string, std::vector<const velox::common::Subfield*>>
      filterSubfields;
  std::vector<SubfieldSpec> subfieldSpecs;
  for (auto& [subfield, _] : filters_) {
    if (auto name = subfield.toString();
        !isSynthesizedColumn(name, infoColumns_) &&
        partitionColumnHandles_.count(name) == 0) {
      VELOX_CHECK(!isSpecialColumn(name));
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
    fieldSpec->setFilter(pair.second);
  }

  if (connectorConfig_->readStatsBasedFilterReorderDisabled(
          connectorQueryCtx_->sessionProperties())) {
    spec->disableStatsBasedFilterReorder();
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
