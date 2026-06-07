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

#pragma once

#include "velox/exec/Operator.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/serializers/PrestoIterativePartitioningSerializer.h"

namespace facebook::velox::exec {

/// Partitioned output operator backed by PrestoIterativePartitioningSerializer.
///
/// Routes each input row to a partition via a hash function, buffers the
/// partitioned data, and flushes serialized Presto pages into the output
/// buffer manager when the buffer is full or the pipeline is draining.
class OptimizedPartitionedOutput : public Operator {
 public:
  /// Minimum flush size for non-final flush; 60 KB + overhead fits a 64 KB
  /// network MTU.
  static constexpr uint64_t kMinDestinationSize = 60 * 1024;

  /// Target number of virtual partitions used to stripe each logical
  /// destination into multiple sub-partitions during partitioning. The
  /// OptimizedHashPartitionFunction picks the largest power-of-two fanout
  /// with numDestinations_ * fanout <= this target; PartitionedVector then
  /// scatters across the expanded id space, breaking the per-cursor
  /// dependency that bottlenecks small destination counts. Set to 0 to
  /// disable virtual partitioning entirely.
  static constexpr uint32_t kVirtualPartitionTarget = 256;

  OptimizedPartitionedOutput(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::PartitionedOutputNode>& planNode);

  void addInput(RowVectorPtr input) override;

  /// Returns true when the operator is not waiting for the output buffer to
  /// drain. The driver checks this before calling addInput() so a blocked
  /// state does not accumulate additional rows.
  bool needsInput() const override;

  /// Always returns nullptr; output is pushed into the buffer manager as a
  /// side-effect. Flushes the serializer when the buffer is full or the
  /// pipeline is draining, then signals noMoreData() once all rows are sent.
  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

 private:
  /// Computes the serializer input columns and the mapping from output columns
  /// to serializer input columns.
  void initializeSerializerLayout();

  /// Builds the RowVector consumed by the serializer. When the output layout
  /// has duplicated columns, this projects only the distinct columns and
  /// leaves duplication to flush time.
  RowVectorPtr prepareSerializerInput(const RowVectorPtr& input) const;

  /// Serializes all buffered rows into Presto pages and enqueues each page
  /// into the output buffer manager. All destinations are always enqueued;
  /// sets blockingReason_ and records a future if the output buffer is full.
  /// Increments numFlushes_ on each call.
  void flush();

  const std::string taskId_;
  const RowTypePtr inputType_;
  const std::vector<column_index_t> keyChannels_;
  /// Non-empty when the output layout differs from the input
  const std::vector<column_index_t> outputChannels_;
  const int32_t numDestinations_;
  /// Total number of virtual destinations the partition function emits ids
  /// into. Equal to numDestinations_ when virtual partitioning is disabled
  /// (non-Hash partition functions); otherwise numDestinations_ * fanout
  /// where fanout is the largest power of two with
  /// numDestinations_ * fanout <= kVirtualPartitionTarget. Set in the
  /// constructor, used to construct partitionFunction_ over the virtual id
  /// space and threaded into the serializer's ctx.numVirtualPartitions so
  /// PartitionedVector::create() takes the virtual scatter path.
  int32_t numVirtualDestinations_;

  const bool replicateNullsAndAny_;
  const std::weak_ptr<exec::OutputBufferManager> bufferManager_;
  /// Holds a reference to the owning task to prevent it from being destroyed
  /// while serialized pages are in flight inside the buffer manager.
  const std::function<void()> bufferReleaseFn_;
  const int64_t maxOutputBufferBytes_;

  velox::memory::MemoryPool* pool_;

  /// Computes per-row partition assignments. Null when numDestinations_ == 1.
  std::unique_ptr<core::PartitionFunction> partitionFunction_;
  /// Reusable buffer for per-row partition assignments.
  std::vector<uint32_t> partitions_;

  std::unique_ptr<serializer::presto::PrestoIterativePartitioningSerializer>
      serializer_;
  /// Row type passed to serializer_->append(). It only includes distinct
  /// columns from the output layout.
  RowTypePtr serializerInputType_;
  /// Input channels that make up the serializer input type. Empty if the output
  /// layout is the same as the input.
  std::vector<column_index_t> serializerInputChannels_;
  /// For each output column index, store the corresponding serializer input
  /// column.
  std::vector<column_index_t> serializerInputByOutput_;

  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  ContinueFuture future_;
  bool finished_{false};

  /// Counts addInput() calls that appended at least one row to the serializer.
  /// Exposed as the "numAppendTimes" runtime stat.
  uint64_t numAppends_{0};
  /// Counts non-empty flush() calls — flushes that serialized at least one
  /// row. Exposed as the "numFlushes" runtime stat for test verification.
  uint64_t numFlushes_{0};
  /// Counts flush() calls that caused the driver to block on a full output
  /// buffer. Exposed as the "numBlockedTimes" runtime stat.
  uint64_t numBlockedTimes_{0};
};

} // namespace facebook::velox::exec
