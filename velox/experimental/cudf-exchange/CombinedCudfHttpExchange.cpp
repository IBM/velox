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
#include "velox/experimental/cudf-exchange/CombinedCudfHttpExchange.h"
#include "velox/experimental/cudf-exchange/Communicator.h"
#include "velox/experimental/cudf-exchange/ExchangeClientFacade.h"
#include "velox/experimental/cudf-exchange/NetUtil.h"
#include "velox/experimental/cudf/exec/Utilities.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace facebook::velox::cudf_exchange {

namespace {
std::unique_ptr<VectorSerde::Options> getVectorSerdeOptions(
    const core::QueryConfig& queryConfig,
    VectorSerde::Kind kind) {
  std::unique_ptr<VectorSerde::Options> options =
      kind == VectorSerde::Kind::kPresto
      ? std::make_unique<serializer::presto::PrestoVectorSerde::PrestoOptions>()
      : std::make_unique<VectorSerde::Options>();
  options->compressionKind =
      common::stringToCompressionKind(queryConfig.shuffleCompressionKind());
  return options;
}
} // namespace

// --- Implementation of the CombinedCudfHttpExchange operator.

CombinedCudfHttpExchange::CombinedCudfHttpExchange(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& planNode,
    std::shared_ptr<ExchangeClientFacade> exchangeClientFacade,
    const std::string& operatorType)
    : SourceOperator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          operatorType),
      preferredOutputBatchBytes_{
          driverCtx->queryConfig().preferredOutputBatchBytes()},
      processSplits_{driverCtx->driverId == 0},
      driverId_{driverCtx->driverId} {
  if (exchangeClientFacade) {
    // cudfExchangeClient is provided externally when this is a "plain"
    // CudfExchange.
    exchangeClient_ = exchangeClientFacade;
    std::shared_ptr<const core::ExchangeNode> exchangeNode =
        std::static_pointer_cast<const core::ExchangeNode>(planNode);
    VELOX_CHECK_NOT_NULL(exchangeNode, "Plan node must be an Exchange node!");
    serdeKind_ = exchangeNode->serdeKind();
    serdeOptions_ = getVectorSerdeOptions(
        operatorCtx_->driverCtx()->queryConfig(), serdeKind_);
  } else {
    // cudfExchangeClient is nullptr when this CudfExchange is used to implement
    // a MergeExchange. Create a new cudf exchange client.
    auto task = operatorCtx_->task();
    auto client = std::make_shared<CudfExchangeClient>(
        task->taskId(),
        task->destination(),
        1, // number of consumers, is always 1.
        task->queryCtx()->executor());
    exchangeClient_ =
        std::make_shared<ExchangeClientFacade>(std::move(client), nullptr);
    exchangeClient_->activateCudfExchangeClient();
  }
}

CombinedCudfHttpExchange::~CombinedCudfHttpExchange() {
  close();
}

void CombinedCudfHttpExchange::addRemoteTaskIds(
    std::vector<std::string>& remoteTaskIds) {
  std::shuffle(std::begin(remoteTaskIds), std::end(remoteTaskIds), rng_);
  for (const std::string& remoteTaskId : remoteTaskIds) {
    exchangeClient_->addRemoteTaskId(remoteTaskId);
  }
  stats_.wlock()->numSplits += remoteTaskIds.size();
}

bool CombinedCudfHttpExchange::getSplits(ContinueFuture* future) {
  if (!processSplits_) {
    return false;
  }
  if (noMoreSplits_) {
    return false;
  }
  std::vector<std::string> remoteTaskIds;
  for (;;) {
    exec::Split split;
    auto reason = operatorCtx_->task()->getSplitOrFuture(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId(), split, *future);
    if (reason == BlockingReason::kNotBlocked) {
      if (split.hasConnectorSplit()) {
        auto remoteSplit = std::dynamic_pointer_cast<RemoteConnectorSplit>(
            split.connectorSplit);
        VELOX_CHECK_NOT_NULL(remoteSplit, "Wrong type of split");
        remoteTaskIds.push_back(remoteSplit->taskId);
      } else {
        addRemoteTaskIds(remoteTaskIds);
        exchangeClient_->noMoreRemoteTasks();
        noMoreSplits_ = true;
        if (atEnd_) {
          operatorCtx_->task()->multipleSplitsFinished(
              false, stats_.rlock()->numSplits, 0);
          recordExchangeClientStats();
        }
        return false;
      }
    } else {
      addRemoteTaskIds(remoteTaskIds);
      return true;
    }
  }
}

bool CombinedCudfHttpExchange::resultIsEmpty() {
  auto checkEmpty = [](auto&& res) -> bool {
    // std::decay_t makes sure that references, const, const references etc.
    // are "decayed" into the base type to allow for the type comparison.
    if constexpr (std::is_same_v<std::decay_t<decltype(res)>, SerPageVector>) {
      return res.empty();
    } else {
      // PackedColPtr
      return res == nullptr;
    }
  };
  return std::visit(checkEmpty, currentData_);
}

void CombinedCudfHttpExchange::emptyResult() {
  auto empty = [](auto&& res) -> void {
    // std::decay_t makes sure that references, const, const references etc.
    // are "decayed" into the base type to allow for the type comparison.
    if constexpr (std::is_same_v<std::decay_t<decltype(res)>, SerPageVector>) {
      res.clear();
    } else {
      // PackedColPtr
      res.reset();
    }
  };
  return std::visit(empty, currentData_);
}

const SerPageVector* CombinedCudfHttpExchange::getResultPages() {
  return std::get_if<SerPageVector>(&currentData_);
}

const PackedColPtr* CombinedCudfHttpExchange::getResultPackedColumns() {
  return std::get_if<PackedColPtr>(&currentData_);
}

BlockingReason CombinedCudfHttpExchange::isBlocked(ContinueFuture* future) {
  if (!resultIsEmpty() || atEnd_) {
    return BlockingReason::kNotBlocked;
  }

  // Get splits from the task if no splits are outstanding.
  if (!splitFuture_.valid()) {
    getSplits(&splitFuture_);
  }

  ContinueFuture dataFuture = ContinueFuture::makeEmpty();
  currentData_ = exchangeClient_->next(
      driverId_, preferredOutputBatchBytes_, &atEnd_, &dataFuture);
  if (!resultIsEmpty() || atEnd_) {
    // got some data or reached the end.
    if (atEnd_ && noMoreSplits_) {
      const auto numSplits = stats_.rlock()->numSplits;
      operatorCtx_->task()->multipleSplitsFinished(false, numSplits, 0);
    }
    recordExchangeClientStats();
    return BlockingReason::kNotBlocked;
  }

  if (splitFuture_.valid()) {
    // Combine the futures and block until data becomes available or more splits
    // arrive.
    std::vector<ContinueFuture> futures;
    futures.push_back(std::move(splitFuture_));
    futures.push_back(std::move(dataFuture));
    *future = folly::collectAny(futures).unit();
    return BlockingReason::kWaitForSplit;
  }

  // Block until data becomes available.
  *future = std::move(dataFuture);
  return BlockingReason::kWaitForProducer;
}

bool CombinedCudfHttpExchange::isFinished() {
  return atEnd_ && resultIsEmpty();
}

// local helper functions for converting the exchange specific format into
// a row vector.
namespace {
std::unique_ptr<folly::IOBuf> mergePages(
    const std::vector<std::unique_ptr<SerializedPage>>& pages) {
  VELOX_CHECK(!pages.empty());
  std::unique_ptr<folly::IOBuf> mergedBufs;
  for (const auto& page : pages) {
    if (mergedBufs == nullptr) {
      mergedBufs = page->getIOBuf();
    } else {
      mergedBufs->appendToChain(page->getIOBuf());
    }
  }
  return mergedBufs;
}
} // namespace

RowVectorPtr CombinedCudfHttpExchange::getOutputFromPages(
    const SerPageVector* pages) {
  VELOX_CHECK_NOT_NULL(pages, "Pages shouldn't be null here.");
  auto* serde = getNamedVectorSerde(serdeKind_);
  RowVectorPtr result = nullptr;
  if (serde->supportsAppendInDeserialize()) {
    uint64_t rawInputBytes{0};
    if (pages->empty()) {
      return nullptr;
    }
    vector_size_t resultOffset = 0;
    for (const auto& page : *pages) {
      rawInputBytes += page->size();

      auto inputStream = page->prepareStreamForDeserialize();
      while (!inputStream->atEnd()) {
        serde->deserialize(
            inputStream.get(),
            pool(),
            outputType_,
            &result,
            resultOffset,
            serdeOptions_.get());
        resultOffset = result->size();
      }
    }
    emptyResult(); // release the memory in the pages vector.
    recordInputStats(rawInputBytes, result);
  } else if (serde->kind() == VectorSerde::Kind::kCompactRow) {
    result = getOutputFromCompactRows(serde, pages);
  } else if (serde->kind() == VectorSerde::Kind::kUnsafeRow) {
    result = getOutputFromUnsafeRows(serde, pages);
  } else {
    VELOX_UNREACHABLE(
        "Unsupported serde kind: {}", VectorSerde::kindName(serde->kind()));
  }
  if (!result) {
    return result;
  }
  // Convert the Velox row vector into a cudf vector.
  auto cudfFromVelox =
      std::make_shared<facebook::velox::cudf_velox::CudfFromVelox>(
          operatorId(), outputType_, operatorCtx_->driverCtx(), planNodeId());
  cudfFromVelox->addInput(result);
  cudfFromVelox->noMoreInput();

  return cudfFromVelox->getOutput();
}

RowVectorPtr CombinedCudfHttpExchange::getOutputFromCompactRows(
    VectorSerde* serde,
    const SerPageVector* pages) {
  uint64_t rawInputBytes{0};
  if (pages->empty()) {
    VELOX_CHECK_NULL(compactRowInputStream_);
    VELOX_CHECK_NULL(compactRowIterator_);
    return nullptr;
  }

  if (compactRowInputStream_ == nullptr) {
    std::unique_ptr<folly::IOBuf> mergedBufs = mergePages(*pages);
    rawInputBytes += mergedBufs->computeChainDataLength();
    compactRowPages_ = std::make_unique<SerializedPage>(std::move(mergedBufs));
    compactRowInputStream_ = compactRowPages_->prepareStreamForDeserialize();
  }

  auto numRows = kInitialOutputCompactRows;
  if (estimatedCompactRowSize_.has_value()) {
    numRows = std::max(
        (preferredOutputBatchBytes_ / estimatedCompactRowSize_.value()),
        kInitialOutputCompactRows);
  }
  RowVectorPtr result = nullptr;
  serde->deserialize(
      compactRowInputStream_.get(),
      compactRowIterator_,
      numRows,
      outputType_,
      &result,
      pool(),
      serdeOptions_.get());
  const auto numOutputRows = result_->size();
  VELOX_CHECK_GT(numOutputRows, 0);

  estimatedCompactRowSize_ = std::max(
      result->estimateFlatSize() / numOutputRows,
      estimatedCompactRowSize_.value_or(1L));

  if (compactRowInputStream_->atEnd() && compactRowIterator_ == nullptr) {
    // only clear the input stream if we have reached the end of the row
    // iterator because row iterator may depend on input stream if serialized
    // rows are not compressed.
    compactRowInputStream_ = nullptr;
    compactRowPages_ = nullptr;
    emptyResult(); // empty current page vector.
  }

  recordInputStats(rawInputBytes, result);
  return result;
}

RowVectorPtr CombinedCudfHttpExchange::getOutputFromUnsafeRows(
    VectorSerde* serde,
    const SerPageVector* pages) {
  uint64_t rawInputBytes{0};
  if (pages->empty()) {
    return nullptr;
  }
  RowVectorPtr result = nullptr;
  std::unique_ptr<folly::IOBuf> mergedBufs = mergePages(*pages);
  rawInputBytes += mergedBufs->computeChainDataLength();
  auto mergedPages = std::make_unique<SerializedPage>(std::move(mergedBufs));
  auto source = mergedPages->prepareStreamForDeserialize();
  serde->deserialize(
      source.get(), pool(), outputType_, &result, serdeOptions_.get());
  emptyResult(); // empty current page vector.
  recordInputStats(rawInputBytes, result);
  return result;
}

RowVectorPtr CombinedCudfHttpExchange::getOutputFromPackedColumns(
    const PackedColPtr* colsPtr) {
  if (*colsPtr == nullptr) {
    return nullptr;
  }
  // Convert the cudf::packed_columns into a RowVectorPtr.
  cudf::table_view tblView = cudf::unpack(**colsPtr);
  // create a new table from the table view and convert that into
  // a CudfVector.
  auto stream =
      facebook::velox::cudf_velox::cudfGlobalStreamPool().get_stream();
  auto mr = cudf::get_current_device_resource_ref();
  std::unique_ptr<cudf::table> tbl =
      std::make_unique<cudf::table>(tblView, stream, mr);
  auto numRows = tbl->num_rows();
  // outputType_ is declared in the Operator base class.
  auto result = std::make_shared<cudf_velox::CudfVector>(
      pool(), outputType_, numRows, std::move(tbl), stream);

  recordInputStats((*colsPtr)->gpu_data->size(), result);
  // free the memory owned by packed_columns and set it to nullptr;
  emptyResult();

  return result;
}

RowVectorPtr CombinedCudfHttpExchange::getOutput() {
  const PackedColPtr* cols = getResultPackedColumns();
  if (cols) {
    return getOutputFromPackedColumns(cols);
  }
  return getOutputFromPages(getResultPages());
}

void CombinedCudfHttpExchange::recordInputStats(
    uint64_t rawInputBytes,
    RowVectorPtr result) {
  auto lockedStats = stats_.wlock();
  lockedStats->rawInputBytes += rawInputBytes;
  // size(): number of rows in result_ vector
  lockedStats->rawInputPositions += result->size();
  lockedStats->addInputVector(result->estimateFlatSize(), result->size());
}

void CombinedCudfHttpExchange::close() {
  SourceOperator::close();
  emptyResult();
  bool usesHttp = false;
  if (exchangeClient_) {
    usesHttp = exchangeClient_->usesHttp_;
    recordExchangeClientStats();
    exchangeClient_->close();
  }
  exchangeClient_ = nullptr;
  if (usesHttp) {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        Operator::kShuffleSerdeKind,
        RuntimeCounter(static_cast<int64_t>(serdeKind_)));
    lockedStats->addRuntimeStat(
        Operator::kShuffleCompressionKind,
        RuntimeCounter(static_cast<int64_t>(serdeOptions_->compressionKind)));
  }
}

void CombinedCudfHttpExchange::recordExchangeClientStats() {
  if (!processSplits_) {
    return;
  }

  auto lockedStats = stats_.wlock();
  const auto exchangeClientStats = exchangeClient_->stats();
  for (const auto& [name, value] : exchangeClientStats) {
    lockedStats->runtimeStats.erase(name);
    lockedStats->runtimeStats.insert({name, value});
  }

  auto backgroundCpuTimeMs =
      exchangeClientStats.find(ExchangeClient::kBackgroundCpuTimeMs);
  if (backgroundCpuTimeMs != exchangeClientStats.end()) {
    const CpuWallTiming backgroundTiming{
        static_cast<uint64_t>(backgroundCpuTimeMs->second.count),
        0,
        static_cast<uint64_t>(backgroundCpuTimeMs->second.sum) *
            Timestamp::kNanosecondsInMillisecond};
    lockedStats->backgroundTiming.clear();
    lockedStats->backgroundTiming.add(backgroundTiming);
  }
}

} // namespace facebook::velox::cudf_exchange
