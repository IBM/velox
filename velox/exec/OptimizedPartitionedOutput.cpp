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

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <unordered_map>

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/SerializedPage.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

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
      replicateNullsAndAny_(planNode->isReplicateNullsAndAny()),
      bufferManager_(DefaultOutputBufferManager::getInstanceRef()),
      // NOTE: 'bufferReleaseFn_' holds a reference on the associated task to
      // prevent it from deleting while there are output buffers being accessed
      // out of the partitioned output buffer manager such as in Prestissimo,
      // the http server holds the buffers while sending the data response.
      bufferReleaseFn_([task = operatorCtx_->task()]() {}),
      maxOutputBufferBytes_(ctx->task->queryCtx()
                                ->queryConfig()
                                .maxPartitionedOutputBufferSize()),
      pool_(pool()),
      partitionFunction_(
          numDestinations_ == 1 ? nullptr
                                : planNode->partitionFunctionSpec().create(
                                      numDestinations_,
                                      /*localExchange=*/false,
                                      true)) {
  if (!planNode->isPartitioned()) {
    VELOX_USER_CHECK_EQ(numDestinations_, 1);
  }
  if (numDestinations_ == 1) {
    VELOX_USER_CHECK(keyChannels_.empty());
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

// The logic comes from PartitionedOutput::collectNullRows().
const SelectivityVector& OptimizedPartitionedOutput::collectNullRows(
    const RowVector& input) {
  const auto size = input.size();
  rows_.resize(size);
  rows_.setAll();

  nullRows_.resize(size);
  nullRows_.clearAll();

  decodedVectors_.resize(keyChannels_.size());

  for (size_t keyChannelIndex = 0; keyChannelIndex < keyChannels_.size();
       ++keyChannelIndex) {
    const auto keyChannel = keyChannels_[keyChannelIndex];
    if (keyChannel == kConstantChannel) {
      continue;
    }
    const auto& keyVector = input.childAt(keyChannel);
    if (keyVector->mayHaveNulls()) {
      DecodedVector& decodedVector = decodedVectors_[keyChannelIndex];
      decodedVector.decode(*keyVector, rows_);
      if (auto* rawNulls = decodedVector.nulls(&rows_)) {
        bits::orWithNegatedBits(
            nullRows_.asMutableRange().bits(), rawNulls, 0, size);
      }
    }
  }
  nullRows_.updateBounds();
  return nullRows_;
}

std::optional<OptimizedPartitionedOutput::ReplicaBatch>
OptimizedPartitionedOutput::prepareReplication(
    const RowVector& input,
    const RowVectorPtr& serializerInput,
    const std::optional<uint32_t>& singlePartition) {
  if (!replicateNullsAndAny_ || numDestinations_ <= 1 || input.size() == 0) {
    return std::nullopt;
  }

  const auto& nullRows = collectNullRows(input);
  replicatedRowIds_.clear();
  if (!replicatedAny_) {
    replicatedRowIds_.push_back(0);
  }
  nullRows.applyToSelected([&](vector_size_t row) {
    // Avoid adding row 0 twice when it has a null key.
    if (replicatedAny_ || row != 0) {
      replicatedRowIds_.push_back(row);
    }
  });
  if (replicatedRowIds_.empty()) {
    return std::nullopt;
  }

  uint32_t mainAppendDestination;
  if (singlePartition.has_value()) {
    mainAppendDestination = singlePartition.value();
  } else {
    // Route replica rows to 0; later appends cover other destinations.
    mainAppendDestination = 0;
    for (const auto row : replicatedRowIds_) {
      partitions_[row] = 0;
    }
  }
  return ReplicaBatch{
      copyRows(serializerInput, replicatedRowIds_), mainAppendDestination};
}

void OptimizedPartitionedOutput::addInput(RowVectorPtr input) {
  auto serializerInput = prepareSerializerInput(input);

  if (serializer_->maxRowsBufferedPerPartition() >=
          kMaxRowsPerDestinationBeforeFlush ||
      serializer_->estimateBytesAfterAppend(serializerInput) >
          maxOutputBufferBytes_) {
    flush();
  }

  auto singlePartition = numDestinations_ == 1
      ? std::optional<uint32_t>{0u}
      : partitionFunction_->partition(*input, partitions_);

  // Prepare replicas before the main append mutates shared input buffers.
  auto replication =
      prepareReplication(*input, serializerInput, singlePartition);

  if (singlePartition.has_value()) {
    serializer_->append(serializerInput, *singlePartition);
  } else {
    serializer_->append(serializerInput, partitions_);
  }

  if (replication.has_value()) {
    replicatedAny_ = true;
    appendReplicatedRows(std::move(replication.value()), 0);
  }

  auto lockedStats = stats_.wlock();
  ++numAppends_;
  lockedStats->addRuntimeStat("numAppends", RuntimeCounter(1));
}

bool OptimizedPartitionedOutput::needsInput() const {
  return blockingReason_ == BlockingReason::kNotBlocked &&
      !pendingReplication_.has_value();
}

RowVectorPtr OptimizedPartitionedOutput::getOutput() {
  if (finished_) {
    return nullptr;
  }

  blockingReason_ = BlockingReason::kNotBlocked;

  if (pendingReplication_.has_value()) {
    auto pending = std::move(pendingReplication_.value());
    pendingReplication_.reset();
    appendReplicatedRows(std::move(pending.batch), pending.nextDestination);
    if (blockingReason_ != BlockingReason::kNotBlocked) {
      return nullptr;
    }
  }

  if (noMoreInput_ || serializer_->bytesBuffered() >= maxOutputBufferBytes_ ||
      serializer_->maxRowsBufferedPerPartition() >=
          kMaxRowsPerDestinationBeforeFlush) {
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

RowVectorPtr OptimizedPartitionedOutput::copyRows(
    const RowVectorPtr& source,
    const std::vector<vector_size_t>& rows) {
  const auto numRows = static_cast<vector_size_t>(rows.size());
  auto copy =
      BaseVector::create<RowVector>(serializerInputType_, numRows, pool_);

  std::vector<BaseVector::CopyRange> ranges;
  vector_size_t start = 0;
  while (start < numRows) {
    vector_size_t end = start + 1;
    while (end < numRows && rows[end] == rows[end - 1] + 1) {
      ++end;
    }
    ranges.push_back({rows[start], start, end - start});
    start = end;
  }
  copy->copyRanges(source.get(), folly::Range(ranges.data(), ranges.size()));
  return copy;
}

RowVectorPtr OptimizedPartitionedOutput::makeReplicaChunk(
    const RowVectorPtr& rowsToReplicate,
    const std::vector<uint32_t>& chunkDestinations) {
  const auto numRows = rowsToReplicate->size();
  const auto numChunkRows = static_cast<vector_size_t>(
      numRows * static_cast<vector_size_t>(chunkDestinations.size()));

  replicaPartitions_.resize(numChunkRows);
  vector_size_t offset = 0;
  for (const auto destination : chunkDestinations) {
    std::fill_n(replicaPartitions_.begin() + offset, numRows, destination);
    offset += numRows;
  }

  if (serializerInputType_->size() == 0) {
    // Row counts come from replicaPartitions_ when there are no columns.
    return std::make_shared<RowVector>(
        pool_,
        serializerInputType_,
        nullptr /*nulls*/,
        numChunkRows,
        std::vector<VectorPtr>{});
  }

  auto indices = allocateIndices(numChunkRows, pool_);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t start = 0; start < numChunkRows; start += numRows) {
    std::iota(rawIndices + start, rawIndices + start + numRows, 0);
  }

  std::vector<VectorPtr> children;
  children.reserve(rowsToReplicate->childrenSize());
  for (const auto& child : rowsToReplicate->children()) {
    children.push_back(
        BaseVector::wrapInDictionary(nullptr, indices, numChunkRows, child));
  }
  return std::make_shared<RowVector>(
      pool_,
      serializerInputType_,
      nullptr /*nulls*/,
      numChunkRows,
      std::move(children));
}

bool OptimizedPartitionedOutput::ensureReplicationAppendCapacity(
    const RowVectorPtr& appendInput) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return false;
  }

  const bool needsFlush = serializer_->maxRowsBufferedPerPartition() >=
          kMaxRowsPerDestinationBeforeFlush ||
      serializer_->estimateBytesAfterAppend(appendInput) >
          maxOutputBufferBytes_ ||
      replicationAppendsSinceLastFlush_ >= kMaxReplicationAppendsPerFlush;
  if (needsFlush) {
    flush();
    return blockingReason_ == BlockingReason::kNotBlocked;
  }
  return true;
}

void OptimizedPartitionedOutput::appendReplicatedRows(
    ReplicaBatch batch,
    uint32_t nextDestination) {
  const int64_t numRows = batch.rowsToReplicate->size();
  VELOX_CHECK_GT(numRows, 0);

  const int64_t destinationsPerChunk =
      std::max<int64_t>(1, kMaxReplicaRowsPerAppend / numRows);

  std::vector<uint32_t> chunkDestinations;
  uint32_t destination = nextDestination;
  while (destination < static_cast<uint32_t>(numDestinations_)) {
    if (destination == batch.mainAppendDestination) {
      ++destination;
      continue;
    }

    chunkDestinations.clear();
    uint32_t next = destination;
    while (next < static_cast<uint32_t>(numDestinations_) &&
           static_cast<int64_t>(chunkDestinations.size()) <
               destinationsPerChunk) {
      if (next != batch.mainAppendDestination) {
        chunkDestinations.push_back(next);
      }
      ++next;
    }

    const bool useDictionaryChunk = chunkDestinations.size() > 1;
    auto appendInput = useDictionaryChunk
        ? makeReplicaChunk(batch.rowsToReplicate, chunkDestinations)
        : batch.rowsToReplicate;
    if (!ensureReplicationAppendCapacity(appendInput)) {
      pendingReplication_ = PendingReplication{std::move(batch), destination};
      return;
    }
    if (useDictionaryChunk) {
      serializer_->append(appendInput, replicaPartitions_);
    } else {
      serializer_->append(appendInput, chunkDestinations[0]);
    }
    stats_.wlock()->addRuntimeStat("numReplicationAppends", RuntimeCounter(1));
    ++replicationAppendsSinceLastFlush_;
    destination = next;
  }
}

void OptimizedPartitionedOutput::flush() {
  replicationAppendsSinceLastFlush_ = 0;

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
  if (flushedRows > 0) {
    lockedStats->addOutputVector(flushedBytes, flushedRows);
    ++numFlushes_;
    lockedStats->addRuntimeStat("numFlushes", RuntimeCounter(1));
  }
  if (shouldBlock) {
    ++numBlockedTimes_;
    lockedStats->addRuntimeStat("numBlockedTimes", RuntimeCounter(1));
  }
}

} // namespace facebook::velox::exec
