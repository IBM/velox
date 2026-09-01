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
#include <optional>
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
class ColumnBufferState;

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
            opts,
            pool,
            {},
            std::move(listenerFactory)) {}

  /// Constructs the serializer with an explicit output-column to input-column
  /// mapping. `outputToInputChannels[i]` indicates which child of the RowVector
  /// passed to append() should be serialized for output column i. When empty,
  /// output column i uses input child i.
  PrestoIterativePartitioningSerializer(
      RowTypePtr outputType,
      uint32_t numPartitions,
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

  /// Routes all rows in `input` to `singlePartition`. Use this overload when
  /// every row in `input` has the same destination — it skips the per-row
  /// partition lookup.
  void append(const RowVectorPtr& input, uint32_t singlePartition);

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

  /// Returns the maximum number of rows buffered in any single partition since
  /// the last flush.
  vector_size_t maxRowsBufferedPerPartition() const;

 private:
  // State threaded through the recursive column flush.
  struct SerializerContext {
    // Number of rows to write per partition at this nesting level. For a
    // top-level column this is the page's row count. For a column nested under
    // a ROW it is the number of rows whose ancestor ROW levels are all
    // non-null.
    std::vector<vector_size_t> rowCounts;

    // Liveness mask per appended batch: a set bit means every ancestor ROW
    // level is non-null for that row, so the row is written. A null entry
    // means every row of the batch is live, which is always the case for
    // top-level columns.
    std::vector<BufferPtr> parentNulls;

    // Number of live rows per partition, per appended batch. Empty for a batch
    // whose 'parentNulls' entry is null, in which case every row of the
    // partition's range is live.
    std::vector<std::vector<vector_size_t>> parentLiveCounts;
  };

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
      const std::vector<IOBufOutputStream*>& outputStreams,
      const SerializerContext& context) const;

  void flushColumn(
      const ColumnBufferState& columnState,
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const TypePtr& colType,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const SerializerContext& context) const;

  void flushSimpleColumn(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const TypePtr& colType,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const SerializerContext& context) const;

  /// Serializes a nested ROW column block. Writes the "ROW" encoding header,
  /// numFields, all child columns recursively, and the Presto ROW block footer
  /// (numRows, offsets, hasNulls flag, optional null bitmap).
  void flushRowColumn(
      const ColumnBufferState& columnState,
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const TypePtr& colType,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const SerializerContext& context) const;

  void flushVariableWidthColumn(
      const ColumnBufferState& columnState,
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const TypePtr& colType,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const SerializerContext& context) const;

  void flushSingleSimpleVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const uint64_t* parentNulls,
      const std::vector<vector_size_t>* parentLiveCounts) const;

  void flushSingleVariableWidthVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  template <TypeKind kind>
  void flushSingleFlatVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const uint64_t* parentNulls) const;

  void flushSingleVariableWidthFlatVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushSingleVariableWidthConstantVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  template <TypeKind kind>
  void flushSingleConstantVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const std::vector<vector_size_t>* parentLiveCounts) const;

  // Writes the values of one partitioned dictionary-encoded fixed-width
  // vector to each partition's stream, omitting rows that are null or whose
  // parent row is null. Values are gathered through the dictionary indices
  // into a scratch chunk before writing.
  template <TypeKind kind>
  void flushSingleDictionaryVector(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const uint64_t* parentNulls) const;

  void flushHeader(
      std::string_view name,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushRowCounts(
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const SerializerContext& context) const;

  void flushNulls(
      const std::vector<PartitionedVectorPtr>& partitionedVectors,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const SerializerContext& context) const;

  // Writes the null section of one block to each partition's stream: the
  // hasNulls flag byte followed, for partitions that have nulls, by the null
  // bitmap. 'validBits[p]' holds the compacted validity bits of the
  // partition's 'rowCounts[p]' written rows in Velox format (LSB first, a set
  // bit means not null); it is converted to the Presto wire format (MSB first,
  // a set bit means null) in place and must own one addressable byte past the
  // last row's bit. Only partitions with 'nullCounts[p]' greater than zero are
  // required to have a non-null 'validBits' entry.
  void flushNullSection(
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const std::vector<vector_size_t>& rowCounts,
      const std::vector<vector_size_t>& nullCounts,
      const std::vector<uint64_t*>& validBits) const;

  // Writes the per-row offsets of one ROW block to each partition's stream.
  // Presto stores one offset per row plus a trailing total; for a ROW block an
  // offset is the running count of non-null rows, so the offsets are
  // sequential for a partition without nulls and a prefix sum over
  // 'validBits[p]' otherwise. See flushNullSection() for 'validBits'.
  void flushRowOffsets(
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams,
      const std::vector<vector_size_t>& rowCounts,
      const std::vector<vector_size_t>& nullCounts,
      const std::vector<uint64_t*>& validBits) const;

  void flushOffsets(
      const ColumnBufferState& columnState,
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  void flushSequentialOffsets(
      const std::vector<uint32_t>& nonEmptyPartitions,
      const std::vector<IOBufOutputStream*>& outputStreams) const;

  RowTypePtr outputType_;
  std::vector<column_index_t> outputToInputChannels_;
  uint32_t numPartitions_;
  SerdeOpts opts_;
  memory::MemoryPool* pool_;

  std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory_;

  /// Number of top-level columns in `outputType_`.
  uint32_t numColumns_{0};

  std::vector<PartitionedVectorPtr> partitionedRowVectors_;

  /// Accumulated state for all batches buffered since the last
  /// flush.
  std::unique_ptr<BufferState> bufferState_;

  // Reusable scratch memory for the chunked value writes in the flush
  // methods.
  mutable Scratch scratch_;
};

} // namespace facebook::velox::serializer::presto
