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

#include "DataSourceBase.h"

#include "ConnectorUtil.h"
#include "velox/dwio/common/ReaderFactory.h"

#include <string>
#include <unordered_map>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::lakehouse::iceberg {

//
DataSourceBase::DataSourceBase(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const connector::ColumnHandleMap& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ConnectorConfigBase>& connectorConfig)
    : connectorQueryCtx_(connectorQueryCtx),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()),
      connectorConfig_(connectorConfig),
      pool_(connectorQueryCtx->memoryPool()),
      outputType_(outputType) {}

void DataSourceBase::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<velox::common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.setFilter(filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }
}

std::unordered_map<std::string, RuntimeMetric>
DataSourceBase::runtimeStats() {
  auto res = runtimeStats_.toRuntimeMetricMap();
  res.insert(
      {{"numPrefetch", RuntimeMetric(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeMetric(
            ioStats_->prefetch().sum(), RuntimeCounter::Unit::kBytes)},
       {"totalScanTime",
        RuntimeMetric(ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
       {Connector::kTotalRemainingFilterTime,
        RuntimeMetric(
            totalRemainingFilterTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)},
       {"ioWaitWallNanos",
        RuntimeMetric(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"maxSingleIoWaitWallNanos",
        RuntimeMetric(
            ioStats_->queryThreadIoLatency().max() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"overreadBytes",
        RuntimeMetric(
            ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)}});
  if (ioStats_->read().count() > 0) {
    res.insert({"numStorageRead", RuntimeMetric(ioStats_->read().count())});
    res.insert(
        {"storageReadBytes",
         RuntimeMetric(ioStats_->read().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ssdRead().count() > 0) {
    res.insert({"numLocalRead", RuntimeMetric(ioStats_->ssdRead().count())});
    res.insert(
        {"localReadBytes",
         RuntimeMetric(
             ioStats_->ssdRead().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ramHit().count() > 0) {
    res.insert({"numRamRead", RuntimeMetric(ioStats_->ramHit().count())});
    res.insert(
        {"ramReadBytes",
         RuntimeMetric(
             ioStats_->ramHit().sum(), RuntimeCounter::Unit::kBytes)});
  }

  const auto fsStats = fsStats_->stats();
  for (const auto& storageStats : fsStats) {
    res.emplace(storageStats.first, storageStats.second);
  }
  return res;
}

void DataSourceBase::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<DataSourceBase*>(sourceUnique.get());
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
}

int64_t DataSourceBase::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

vector_size_t DataSourceBase::evaluateRemainingFilter(
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

bool isSpecialColumn(const std::string& name) {
  return false;
}

void DataSourceBase::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}

} // namespace facebook::velox::connector::lakehouse::iceberg
