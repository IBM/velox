/*
 * Copyright (c) International Business Machines Corporation
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

#include "velox/exec/OptimizedPartitionedOutput.h"

#include <unordered_map>

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/SerializedPage.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

namespace {

// Returns the largest power of two with numDestinations * fanout <= target,
// or 1 to disable virtual partitioning when numDestinations < 2 or already
// meets the target.
uint32_t pickVirtualPartitionFanout(uint32_t numDestinations, uint32_t target) {
  if (numDestinations < 2 || target <= numDestinations) {
    return 1;
  }
  uint32_t fanout = 1;
  while (numDestinations * fanout * 2 <= target) {
    fanout *= 2;
  }
  return fanout;
}

} // namespace

OptimizedPartitionedOutput::OptimizedPartitionedOutput(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "OptimizedPartitionedOutput"),
      taskId_(operatorCtx_->taskId()),
      inputType_(planNode->inputType()),
      keyChannels_(toChannels(planNode->inputType(), planNode->keys())),
      outputChannels_(calculateOutputChannels(
          planNode->inputType(),
          planNode->outputType(),
          planNode->outputType())),
      numDestinations_(planNode->numPartitions()),
      // Set below, after partitionFunction_ is constructed. Defaults to
      // numDestinations_ so the serializer behaves as if virtual
      // partitioning is disabled when the partition function isn't an
      // OptimizedHashPartitionFunction.
      numVirtualDestinations_(numDestinations_),
      replicateNullsAndAny_(planNode->isReplicateNullsAndAny()),
      bufferManager_(OutputBufferManager::getInstanceRef()),
      // NOTE: 'bufferReleaseFn_' holds a reference on the associated task to
      // prevent it from deleting while there are output buffers being accessed
      // out of the partitioned output buffer manager such as in Prestissimo,
      // the http server holds the buffers while sending the data response.
      bufferReleaseFn_([task = operatorCtx_->task()]() {}),
      maxOutputBufferBytes_(ctx->task->queryCtx()
                                ->queryConfig()
                                .maxPartitionedOutputBufferSize()),
      pool_(pool()) {
  if (!planNode->isPartitioned()) {
    VELOX_USER_CHECK_EQ(numDestinations_, 1);
  }
  if (numDestinations_ == 1) {
    VELOX_USER_CHECK(keyChannels_.empty());
  }

  if (numDestinations_ > 1) {
    // Virtual partitioning is only applied when the partition function
    // spec is a HashPartitionFunctionSpec (which combined with
    // useOptimizedPartitionFunction=true returns an
    // OptimizedHashPartitionFunction). Other spec types
    // (GatherPartitionFunctionSpec, RoundRobinPartitionFunctionSpec, etc.)
    // keep numVirtualDestinations_ == numDestinations_, which disables
    // virtual partitioning in the serializer.
    if (dynamic_cast<const HashPartitionFunctionSpec*>(
            &planNode->partitionFunctionSpec()) != nullptr) {
      const uint32_t fanout = pickVirtualPartitionFanout(
          static_cast<uint32_t>(numDestinations_), kVirtualPartitionTarget);
      numVirtualDestinations_ = numDestinations_ * static_cast<int32_t>(fanout);
    }
    // Construct the partition function over the virtual id space directly:
    // pass numVirtualDestinations_ as the partition count so partition()
    // natively emits ids in [0, numVirtualDestinations_). addInput() then
    // doesn't have to re-map the ids before passing them to the serializer.
    partitionFunction_ = planNode->partitionFunctionSpec().create(
        numVirtualDestinations_,
        /*localExchange=*/false,
        /*useOptimizedPartitionFunction=*/true);
  }

  serializer::presto::SerdeOpts options;
  options.compressionKind = common::stringToCompressionKind(
      operatorCtx_->driverCtx()->queryConfig().shuffleCompressionKind());
  options.minCompressionRatio = 0.8;

  initializeSerializerLayout();

  serializer_ = std::make_unique<
      serializer::presto::PrestoIterativePartitioningSerializer>(
      outputType_,
      numDestinations_,
      static_cast<uint32_t>(numVirtualDestinations_),
      options,
      pool_,
      serializerInputByOutput_,
      [bufferManager =
           bufferManager_]() -> std::unique_ptr<OutputStreamListener> {
        auto lockedBufferManager = bufferManager.lock();
        VELOX_CHECK_NOT_NULL(
            lockedBufferManager, "OutputBufferManager was already destructed");
        return lockedBufferManager->newListener();
      });
}

void OptimizedPartitionedOutput::addInput(RowVectorPtr input) {
  VELOX_USER_CHECK(
      !replicateNullsAndAny_,
      "replicateNullsAndAny is not yet supported by OptimizedPartitionedOutput");

  auto serializerInput = prepareSerializerInput(input);

  if (serializer_->estimateBytesAfterAppend(serializerInput) >
      maxOutputBufferBytes_) {
    flush();
  }

  const auto numRows = input->size();
  partitions_.resize(numRows);

  if (numDestinations_ == 1) {
    std::fill(partitions_.begin(), partitions_.end(), 0u);
  } else {
    // partitionFunction_ was constructed over the virtual id space
    // (numVirtualDestinations_), so it natively emits virtual ids in
    // [0, numVirtualDestinations_). No striping required here — the
    // serializer's ctx.numVirtualPartitions == numVirtualDestinations_
    // matches.
    std::optional<uint32_t> partition =
        partitionFunction_->partition(*input, partitions_);
    if (partition.has_value()) {
      // All rows go to the same partition
      std::fill(partitions_.begin(), partitions_.end(), partition.value());
    }
  }

  serializer_->append(serializerInput, partitions_);

  auto lockedStats = stats_.wlock();
  ++numAppends_;
  lockedStats->addRuntimeStat("numAppends", RuntimeCounter(1));
}

bool OptimizedPartitionedOutput::needsInput() const {
  return blockingReason_ == BlockingReason::kNotBlocked;
}

RowVectorPtr OptimizedPartitionedOutput::getOutput() {
  if (finished_) {
    return nullptr;
  }

  blockingReason_ = BlockingReason::kNotBlocked;

  if (noMoreInput_ || serializer_->bytesBuffered() >= maxOutputBufferBytes_) {
    flush();
  }

  // If blocked, stop here. We avoid advancing operator state while blocked,
  // even if noMoreInput_ may already be true. The driver will resume and call
  // getOutput() again once the OutputBuffer has space.
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return nullptr;
  }

  if (noMoreInput_ && serializer_->bytesBuffered() == 0) {
    // TODO: merge serializer runtime stats into operator stats once
    // PrestoIterativePartitioningSerializer exposes runtimeStats().
    bufferManager_.lock()->noMoreData(operatorCtx_->task()->taskId());
    finished_ = true;
  }

  return nullptr;
}

BlockingReason OptimizedPartitionedOutput::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    blockingReason_ = BlockingReason::kNotBlocked;
    return BlockingReason::kWaitForConsumer;
  }
  return BlockingReason::kNotBlocked;
}

bool OptimizedPartitionedOutput::isFinished() {
  return finished_;
}

void OptimizedPartitionedOutput::initializeSerializerLayout() {
  if (outputType_->size() == 0 || outputChannels_.empty()) {
    serializerInputType_ = outputType_;
    return;
  }

  std::unordered_map<column_index_t, column_index_t> outputToSerializerInput;
  outputToSerializerInput.reserve(outputChannels_.size());

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(outputChannels_.size());
  types.reserve(outputChannels_.size());
  serializerInputByOutput_.reserve(outputChannels_.size());

  for (const auto outputChannel : outputChannels_) {
    auto it = outputToSerializerInput.find(outputChannel);
    if (it == outputToSerializerInput.end()) {
      const auto serializerInputChannel =
          static_cast<column_index_t>(serializerInputChannels_.size());
      serializerInputChannels_.push_back(outputChannel);
      names.push_back(inputType_->nameOf(outputChannel));
      types.push_back(inputType_->childAt(outputChannel));
      it =
          outputToSerializerInput.emplace(outputChannel, serializerInputChannel)
              .first;
    }
    serializerInputByOutput_.push_back(it->second);
  }

  serializerInputType_ = ROW(std::move(names), std::move(types));
}

RowVectorPtr OptimizedPartitionedOutput::prepareSerializerInput(
    const RowVectorPtr& input) const {
  VELOX_CHECK_NOT_NULL(input);

  if (serializerInputType_->size() == 0) {
    return std::make_shared<RowVector>(
        input->pool(),
        serializerInputType_,
        nullptr /*nulls*/,
        input->size(),
        std::vector<VectorPtr>{});
  }

  if (serializerInputChannels_.empty()) {
    input->loadedVector();
    return input;
  }

  std::vector<VectorPtr> serializerInputColumns;
  serializerInputColumns.reserve(serializerInputChannels_.size());
  for (auto channel : serializerInputChannels_) {
    auto loadedChild = BaseVector::loadedVectorShared(input->childAt(channel));
    serializerInputColumns.push_back(loadedChild);
  }

  return std::make_shared<RowVector>(
      input->pool(),
      serializerInputType_,
      nullptr /*nulls*/,
      input->size(),
      std::move(serializerInputColumns));
}

void OptimizedPartitionedOutput::flush() {
  const auto flushedBytes = serializer_->bytesBuffered();
  const auto flushedRows = serializer_->rowsBuffered();

  // This will serialize all destinations and reset serializer_->bytesBuffered()
  // to 0.
  auto serializedIOBufs = serializer_->flush();
  auto bufferManager = bufferManager_.lock();
  VELOX_CHECK_NOT_NULL(
      bufferManager, "OutputBufferManager was already destructed");

  bool shouldBlock = false;
  ContinueFuture future = ContinueFuture::makeEmpty();
  for (auto& [destination, pageData] : serializedIOBufs) {
    // We will only pass the future to bufferManager->enqueue() for the first
    // blocked destination. This is to avoid unnecessary creation of
    // ContinueFuture objects for the remaining destinations.
    ContinueFuture* futurePtr = shouldBlock ? nullptr : &future;

    // Enqueue the data for each non-empty partition. Since the pageData is
    // already serialized, enqueueing them would not cause new memory
    // allocations. This will always move the pageData to the OutputBuffers no
    // matter if the OutputBuffer is blocked.
    bool blocked = bufferManager->enqueue(
        taskId_,
        static_cast<int>(destination),
        std::make_unique<PrestoSerializedPage>(
            std::move(pageData.first),
            [fn = bufferReleaseFn_](folly::IOBuf&) { fn(); },
            pageData.second),
        futurePtr);

    if (blocked && !shouldBlock) {
      blockingReason_ = BlockingReason::kWaitForConsumer;
      shouldBlock = true;
      future_ = std::move(future);
    }
  }

  auto lockedStats = stats_.wlock();
  lockedStats->addOutputVector(flushedBytes, flushedRows);
  if (flushedRows > 0) {
    ++numFlushes_;
    lockedStats->addRuntimeStat("numFlushes", RuntimeCounter(1));
  }
  if (shouldBlock) {
    ++numBlockedTimes_;
    lockedStats->addRuntimeStat("numBlockedTimes", RuntimeCounter(1));
  }
}

} // namespace facebook::velox::exec
