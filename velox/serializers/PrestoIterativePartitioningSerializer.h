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

#include <functional>
#include <map>
#include <memory>
#include <vector>

#include <folly/io/IOBuf.h>

#include "velox/common/memory/ByteStream.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/PartitionedVector.h"

namespace facebook::velox::serializer::presto {

/// Convenience alias matching PrestoSerializer.cpp convention.
using SerdeOpts = PrestoVectorSerde::PrestoOptions;

class BufferState;

/// Serializes a stream of RowVectors into per-partition Presto pages.
///
/// Each call to append() routes rows to their assigned partition. flush()
/// produces one Presto-format IOBuf per non-empty partition and resets the
/// internal state so the serializer can be reused for the next cycle.
class PrestoIterativePartitioningSerializer {
 public:
  PrestoIterativePartitioningSerializer(
      RowTypePtr outputType,
      uint32_t numPartitions,
      const SerdeOpts& opts,
      memory::MemoryPool* pool)
      : PrestoIterativePartitioningSerializer(
            std::move(outputType),
            numPartitions,
            /*numVirtualPartitions=*/0,
            opts,
            pool,
            {},
            nullptr) {}

  /// Constructs the serializer. If `listenerFactory` is non-null it is called
  /// once per non-empty partition on each flush to create an
  /// OutputStreamListener that accumulates the CRC32 checksum; the checksum
  /// bit is then set in the Presto page codec byte and the computed value is
  /// written into the page header. Pass nullptr to skip checksum computation,
  /// which matches the behavior of kNormal PartitionedOutput when
  /// OutputBufferManager has no listener factory set.
  PrestoIterativePartitioningSerializer(
      RowTypePtr outputType,
      uint32_t numPartitions,
      const SerdeOpts& opts,
      memory::MemoryPool* pool,
      std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory)
      : PrestoIterativePartitioningSerializer(
            std::move(outputType),
            numPartitions,
            /*numVirtualPartitions=*/0,
            opts,
            pool,
            {},
            std::move(listenerFactory)) {}

  /// Constructs the serializer with an explicit output-column to input-column
  /// mapping. `outputToInputChannels[i]` indicates which child of the RowVector
  /// passed to append() should be serialized for output column i. When empty,
  /// output column i uses input child i. `numVirtualPartitions` controls
  /// virtual partitioning of the downstream PartitionedVector::create():
  /// pass 0 (the default) or `numPartitions` for no virtual partitioning;
  /// pass `numPartitions * fanout` (fanout a power of two > 1) when the
  /// caller's partition function has already striped its ids across `fanout`
  /// virtual sub-partitions per logical partition.
  PrestoIterativePartitioningSerializer(
      RowTypePtr outputType,
      uint32_t numPartitions,
      uint32_t numVirtualPartitions,
      const SerdeOpts& opts,
      memory::MemoryPool* pool,
      std::vector<column_index_t> outputToInputChannels,
      std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory =
          nullptr);

  ~PrestoIterativePartitioningSerializer();

  /// Returns a conservative estimate of bytesBuffered() after appending
  /// `input`. The partition assignment of the input is not known at the time of
  /// the call, so this assumes worst-case growth from new non-empty partitions
  /// and may overestimate.
  int64_t estimateBytesAfterAppend(const RowVectorPtr& input) const;

  /// Routes each row in `input` to the partition indicated by `partitions`.
  /// `partitions.size()` must equal `input->size()`.
  void append(
      const RowVectorPtr& input,
      const std::vector<uint32_t>& partitions);

  /// Serializes all buffered data into one Presto page per non-empty partition
  /// and resets internal state. Returns an empty map if nothing has been
  /// appended since the last flush.
  std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
  flush();

  /// Returns the serialized bytes buffered across all partitions since the last
  /// flush.
  int64_t bytesBuffered() const;

  /// Returns the total number of rows appended since the last flush.
  vector_size_t rowsBuffered() const;

 private:
  void validateOutputInputMapping(const RowVectorPtr&) const;

  column_index_t outputToInputChannel(column_index_t outputColumn) const {
    return outputToInputChannels_.empty()
        ? outputColumn
        : outputToInputChannels_[outputColumn];
  }

  std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
  flushUncompressed();
  std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
  flushCompressed();

  void clear();

  void flushStart(IOBufOutputStream& out, uint32_t partition, char codecMask)
      const;

  void flushFinish(
      IOBufOutputStream& out,
      uint32_t partition,
      std::streampos beginOffset,
      char codecMask,
      OutputStreamListener* listener) const;

  void flushRowChildren(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const RowType& rowSchema,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushColumn(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const TypePtr& colType,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushSimpleColumn(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const TypePtr& colType,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushSingleSimpleVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  template <TypeKind kind>
  void flushSingleFlatVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  template <TypeKind kind>
  void flushSingleConstantVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushHeader(
      std::string_view name,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushRowCounts(
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushNulls(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  static void flushSimpleVectorNulls(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<uint32_t>& nonEmptyPartitions,
      std::vector<std::vector<uint8_t>>& bitmaps,
      std::vector<vector_size_t>& destBitOffsets);

  static void flushConstantVectorNulls(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<uint32_t>& nonEmptyPartitions,
      std::vector<std::vector<uint8_t>>& bitmaps,
      std::vector<vector_size_t>& destBitOffsets);

  template <typename T>
  void flushFlatValues(
      const T* partitionedValues,
      const uint64_t* rawNulls,
      const vector_size_t* partitionOffsets,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushSequentialOffsets(
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  RowTypePtr outputType_;
  std::vector<column_index_t> outputToInputChannels_;
  uint32_t numPartitions_;
  // Set by the constructor (defaulting to numPartitions_ when the caller
  // passes 0). When > numPartitions_, append() forwards it via
  // ctx.numVirtualPartitions so PartitionedVector::create() takes the
  // virtual-partitioning scatter path.
  uint32_t numVirtualPartitions_;
  SerdeOpts opts_;
  memory::MemoryPool* pool_;

  std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory_;

  /// Number of top-level columns in `outputType_`.
  uint32_t numColumns_{0};

  std::vector<PartitionedVectorPtr> partitionedRowVectors_;

  /// Accumulated state for all batches buffered since the last
  /// flush.
  std::unique_ptr<BufferState> bufferState_;
};

} // namespace facebook::velox::serializer::presto
