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
#include "velox/serializers/PrestoIterativePartitioningSerializer.h"

#include <algorithm>
#include <optional>

#include "velox/common/base/BitUtil.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::serializer::presto {

namespace {

constexpr int8_t kCheckSumBitMask = 4;
constexpr int64_t kVectorSizeTypeSize{sizeof(vector_size_t)};
// [numRows:4][codec:1]
constexpr int64_t kUncompressedSizeOffset{kVectorSizeTypeSize + 1};
// [numRows:4][codec:1][uncompressedSize:4][compressedSize:4][checksum:8]
constexpr int64_t kHeaderSize{kUncompressedSizeOffset + 4 + 4 + 8};

// chunk size for flushing constant values
constexpr int32_t kChunkBytes = 4096;

static inline const std::string_view kByteArray{"BYTE_ARRAY"};
static inline const std::string_view kShortArray{"SHORT_ARRAY"};
static inline const std::string_view kIntArray{"INT_ARRAY"};
static inline const std::string_view kLongArray{"LONG_ARRAY"};
static inline const std::string_view kInt128Array{"INT128_ARRAY"};
static inline const std::string_view kVariableWidth{"VARIABLE_WIDTH"};
static inline const std::string_view kRow{"ROW"};

inline void writeInt32(OutputStream* out, int32_t value) {
  out->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

inline void writeInt64(OutputStream* out, int64_t value) {
  out->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

char getCodecMarker(bool checksumEnabled) {
  char marker = 0;
  if (checksumEnabled) {
    marker |= kCheckSumBitMask;
  }
  return marker;
}

std::string_view typeToEncodingName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return kByteArray;
    case TypeKind::SMALLINT:
      return kShortArray;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return kIntArray;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
      return kLongArray;
    case TypeKind::HUGEINT:
      return kInt128Array;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return kVariableWidth;
    case TypeKind::ROW:
      return kRow;
    default:
      VELOX_FAIL("Unsupported type kind: {}", static_cast<int>(type->kind()));
  }
}

/// Finalizes the Presto page CRC by mixing in the codec marker, row count,
/// and uncompressed size on top of the listener's accumulated data checksum.
int64_t computeChecksum(
    PrestoOutputStreamListener& listener,
    int8_t codecMarker,
    int32_t numRows,
    int32_t uncompressedSize) {
  auto crc = listener.crc();
  crc.process_bytes(&codecMarker, 1);
  crc.process_bytes(&numRows, 4);
  crc.process_bytes(&uncompressedSize, 4);
  return static_cast<int64_t>(crc.checksum());
}

/// Returns the serialized byte width of a fixed-width type, matching the
/// sizeof(T) used in flushFlatValues.
int32_t fixedTypeWidth(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::TIMESTAMP:
    case TypeKind::HUGEINT:
      return 16;
    default:
      return 0;
  }
}

/// Returns the exact bytes for one fixed-width column in one partition.
int64_t
simpleColumnBytes(const TypePtr& colType, int64_t numRows, int64_t numNulls) {
  const auto encodingName = typeToEncodingName(colType);
  return 4 + static_cast<int64_t>(encodingName.size()) + // header
      4 + // rowCount
      1 + // nullFlag
      (numNulls > 0 ? bits::nbytes(numRows) : 0) + // null bitmap
      (numRows - numNulls) * fixedTypeWidth(colType->kind()); // values
}

/// Returns the null counts if it can be derived without row-by-row checks,
/// otherwise returns std::nullopt.
std::optional<vector_size_t> countNulls(const BaseVector& vector) {
  if (!vector.mayHaveNulls()) {
    return 0;
  }

  if (const auto nullCount = vector.getNullCount()) {
    return *nullCount;
  }

  switch (vector.encoding()) {
    case VectorEncoding::Simple::FLAT:
    case VectorEncoding::Simple::ROW:
      return BaseVector::countNulls(vector.nulls(), vector.size());
    case VectorEncoding::Simple::CONSTANT:
      return vector.isNullAt(0) ? vector.size() : 0;
    case VectorEncoding::Simple::DICTIONARY: {
      vector_size_t nullCount = 0;
      for (auto i = 0; i < vector.size(); ++i) {
        nullCount += vector.isNullAt(i);
      }
      return nullCount;
    }
    default:
      return std::nullopt;
  }
}

/// Returns the maximum null-bitmap bytes for totalRows distributed across
/// numPartitionsWithNulls partitions. This occurs when one row is put in each
/// partition first, then one byte is added for every 8 remaining rows.
int64_t maxBitmapBytes(int64_t totalRows, int64_t numPartitionsWithNulls) {
  if (numPartitionsWithNulls == 0) {
    return 0;
  }
  VELOX_DCHECK_LE(numPartitionsWithNulls, totalRows);
  return numPartitionsWithNulls + (totalRows - numPartitionsWithNulls) / 8;
}

/// Base class for column nodes in the serializer's per-partition accounting.
///
/// A node tracks exact row, null, and byte counts for one column while
/// appending partitioned vectors.
class ColumnBufferState {
 public:
  ColumnBufferState(TypePtr type, uint32_t numPartitions)
      : type_(std::move(type)),
        numPartitions_(numPartitions),
        rowsPerPartition_(numPartitions, 0),
        nullsPerPartition_(numPartitions, 0),
        bytesPerPartition_(numPartitions, 0) {}

  virtual ~ColumnBufferState() = default;

  static std::unique_ptr<ColumnBufferState> create(
      const TypePtr& type,
      uint32_t numPartitions);

  virtual void append(const PartitionedVectorPtr& partitionedVector) = 0;

  virtual void clear() {
    std::fill(rowsPerPartition_.begin(), rowsPerPartition_.end(), 0);
    std::fill(nullsPerPartition_.begin(), nullsPerPartition_.end(), 0);
    std::fill(bytesPerPartition_.begin(), bytesPerPartition_.end(), 0);
    numNonEmptyPartitions_ = 0;
    numPartitionsWithNulls_ = 0;
  }

  const std::vector<vector_size_t>& rowsPerPartition() const {
    return rowsPerPartition_;
  }

  const std::vector<int64_t>& bytesPerPartition() const {
    return bytesPerPartition_;
  }

  uint32_t numNonEmptyPartitions() const {
    return numNonEmptyPartitions_;
  }

  uint32_t numPartitionsWithNulls() const {
    return numPartitionsWithNulls_;
  }

  int64_t nullBitmapBytesBuffered() const {
    int64_t total = 0;
    for (auto p = 0; p < numPartitions_; ++p) {
      if (nullsPerPartition_[p] > 0) {
        total += bits::nbytes(rowsPerPartition_[p]);
      }
    }
    return total;
  }

 protected:
  const TypePtr type_;
  const uint32_t numPartitions_;
  std::vector<vector_size_t> rowsPerPartition_;
  std::vector<vector_size_t> nullsPerPartition_;
  std::vector<int64_t> bytesPerPartition_;

  // count of partitions with at least one buffered row
  uint32_t numNonEmptyPartitions_{0};

  // count of partitions that require a null bitmap
  uint32_t numPartitionsWithNulls_{0};
};

/// Buffer state for one fixed-width column.
class FixedWidthBufferState : public ColumnBufferState {
 public:
  FixedWidthBufferState(TypePtr type, uint32_t numPartitions)
      : ColumnBufferState(std::move(type), numPartitions) {}

  void append(const PartitionedVectorPtr& partitionedVector) override {
    for (auto p = 0; p < numPartitions_; ++p) {
      const auto numRows = partitionedVector->numRowsAt(p);
      if (numRows == 0) {
        continue;
      }

      const auto numNulls = partitionedVector->numNullsAt(p);
      auto& rows = rowsPerPartition_[p];
      auto& nulls = nullsPerPartition_[p];

      if (rows == 0) {
        ++numNonEmptyPartitions_;
      }
      if (nulls == 0 && numNulls > 0) {
        ++numPartitionsWithNulls_;
      }
      rows += numRows;
      nulls += numNulls;
      bytesPerPartition_[p] = simpleColumnBytes(type_, rows, nulls);
    }
  }
};

/// Buffer state for one VARCHAR or VARBINARY column.
class VariableWidthBufferState : public ColumnBufferState {
 public:
  VariableWidthBufferState(TypePtr type, uint32_t numPartitions)
      : ColumnBufferState(std::move(type), numPartitions) {}

  void append(const PartitionedVectorPtr& partitionedVector) override {
    VELOX_NYI(
        "Variable-width columns are not yet supported by "
        "PrestoIterativePartitioningSerializer::append");
  }
};

std::unique_ptr<ColumnBufferState> ColumnBufferState::create(
    const TypePtr& type,
    uint32_t numPartitions) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::HUGEINT:
      return std::make_unique<FixedWidthBufferState>(type, numPartitions);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return std::make_unique<VariableWidthBufferState>(type, numPartitions);
    case TypeKind::TIMESTAMP:
    case TypeKind::ROW:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
      VELOX_NYI(
          "Unsupported type kind for createColumnBufferState: {}",
          type->kind());
    default:
      VELOX_UNSUPPORTED(
          "Unsupported type kind for createColumnBufferState: {}",
          type->kind());
  }
}

} // namespace

/// Top-level buffer state for one output page.
///
/// For each partition, tracks page-level headers and aggregates child column
/// sizes.
class BufferState {
 public:
  BufferState(
      uint32_t numPartitions,
      std::vector<std::unique_ptr<ColumnBufferState>> children)
      : numPartitions_(numPartitions),
        rowsPerPartition_(numPartitions, 0),
        bytesPerPartition_(numPartitions, 0),
        children_(std::move(children)) {}

  static std::unique_ptr<BufferState> create(
      const RowTypePtr& type,
      uint32_t numPartitions);

  void append(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<column_index_t>& outputToInputChannels) {
    auto rowVector =
        std::dynamic_pointer_cast<PartitionedRowVector>(partitionedVector);
    VELOX_CHECK_NOT_NULL(rowVector);

    rowsBuffered_ += partitionedVector->baseVector()->size();

    for (column_index_t column = 0; column < children_.size(); ++column) {
      const auto inputColumn = outputToInputChannels.empty()
          ? column
          : outputToInputChannels[column];
      children_[column]->append(rowVector->childAt(inputColumn));
    }

    for (auto p = 0; p < numPartitions_; ++p) {
      const auto numRows = partitionedVector->numRowsAt(p);
      if (numRows == 0) {
        continue;
      }
      if (rowsPerPartition_[p] == 0) {
        ++numNonEmptyPartitions_;
      }
      rowsPerPartition_[p] += numRows;

      int64_t partitionBytes = kHeaderSize + 4;
      for (const auto& child : children_) {
        partitionBytes += child->bytesPerPartition()[p];
      }
      bytesBuffered_ += partitionBytes - bytesPerPartition_[p];
      bytesPerPartition_[p] = partitionBytes;
    }
  }

  void clear() {
    std::fill(rowsPerPartition_.begin(), rowsPerPartition_.end(), 0);
    std::fill(bytesPerPartition_.begin(), bytesPerPartition_.end(), 0);
    numNonEmptyPartitions_ = 0;
    rowsBuffered_ = 0;
    bytesBuffered_ = 0;
    for (auto& child : children_) {
      child->clear();
    }
  }

  const std::vector<vector_size_t>& rowsPerPartition() const {
    return rowsPerPartition_;
  }

  const std::vector<int64_t>& bytesPerPartition() const {
    return bytesPerPartition_;
  }

  uint32_t numNonEmptyPartitions() const {
    return numNonEmptyPartitions_;
  }

  vector_size_t rowsBuffered() const {
    return rowsBuffered_;
  }

  int64_t bytesBuffered() const {
    return bytesBuffered_;
  }

  const std::vector<std::unique_ptr<ColumnBufferState>>& children() const {
    return children_;
  }

 private:
  const uint32_t numPartitions_;
  std::vector<vector_size_t> rowsPerPartition_;
  std::vector<int64_t> bytesPerPartition_;
  uint32_t numNonEmptyPartitions_{0};
  vector_size_t rowsBuffered_{0};
  int64_t bytesBuffered_{0};
  std::vector<std::unique_ptr<ColumnBufferState>> children_;
};

std::unique_ptr<BufferState> BufferState::create(
    const RowTypePtr& type,
    uint32_t numPartitions) {
  std::vector<std::unique_ptr<ColumnBufferState>> children;
  children.reserve(type->size());
  for (auto column = 0; column < type->size(); ++column) {
    children.push_back(
        ColumnBufferState::create(type->childAt(column), numPartitions));
  }
  return std::make_unique<BufferState>(numPartitions, std::move(children));
}

PrestoIterativePartitioningSerializer::PrestoIterativePartitioningSerializer(
    RowTypePtr outputType,
    uint32_t numPartitions,
    uint32_t numVirtualPartitions,
    const SerdeOpts& opts,
    memory::MemoryPool* pool,
    std::vector<column_index_t> outputToInputChannels,
    std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory)
    : outputType_(std::move(outputType)),
      outputToInputChannels_(std::move(outputToInputChannels)),
      numPartitions_(numPartitions),
      numVirtualPartitions_(
          numVirtualPartitions == 0 ? numPartitions : numVirtualPartitions),
      opts_(opts),
      pool_(pool),
      listenerFactory_(std::move(listenerFactory)),
      numColumns_(outputType_->size()),
      bufferState_(BufferState::create(outputType_, numPartitions_)) {
  VELOX_CHECK_GT(numPartitions_, 0);
  VELOX_CHECK_GE(numVirtualPartitions_, numPartitions_);
  VELOX_CHECK_EQ(numVirtualPartitions_ % numPartitions_, 0);
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK(
      outputToInputChannels_.empty() ||
          outputToInputChannels_.size() == outputType_->size(),
      "outputToInputChannels size must match output column count");
}

PrestoIterativePartitioningSerializer::
    ~PrestoIterativePartitioningSerializer() = default;

int64_t PrestoIterativePartitioningSerializer::bytesBuffered() const {
  return bufferState_->bytesBuffered();
}

vector_size_t PrestoIterativePartitioningSerializer::rowsBuffered() const {
  return bufferState_->rowsBuffered();
}

void PrestoIterativePartitioningSerializer::clear() {
  partitionedRowVectors_.clear();
  bufferState_->clear();
}

void PrestoIterativePartitioningSerializer::validateOutputInputMapping(
    const RowVectorPtr& input) const {
  const auto numInputColumns = input->childrenSize();
  for (column_index_t outputColumn = 0; outputColumn < numColumns_;
       ++outputColumn) {
    const auto inputColumn = outputToInputChannel(outputColumn);
    VELOX_CHECK_LT(
        inputColumn,
        numInputColumns,
        "Output column {} maps to invalid input column {}",
        outputColumn,
        inputColumn);

    const auto& child = input->childAt(inputColumn);
    VELOX_CHECK_NOT_NULL(
        child,
        "Output column {} maps to null input column {}",
        outputColumn,
        inputColumn);

    const auto type = outputType_->childAt(outputColumn);
    VELOX_CHECK(
        child->type()->equivalent(*type),
        "Output column {} expects {}, got {} from input column {}",
        outputColumn,
        type->toString(),
        child->type()->toString(),
        inputColumn);
  }
}

int64_t PrestoIterativePartitioningSerializer::estimateBytesAfterAppend(
    const RowVectorPtr& input) const {
  VELOX_CHECK_NOT_NULL(input);
  validateOutputInputMapping(input);

  if (input->size() == 0) {
    return bytesBuffered();
  }

  const auto numRows = input->size();

  // Worst case: each input row lands in a distinct empty partition, capped by
  // the number of empty partitions.
  const auto numNewPartitions = std::min<uint32_t>(
      numRows, numPartitions_ - bufferState_->numNonEmptyPartitions());
  // One page header per newly non-empty partition.
  auto estimatedBytes =
      bufferState_->bytesBuffered() + numNewPartitions * (kHeaderSize + 4);

  // Cache per input column. If multiple output columns map to the same input
  // column, reuse the already computed incremental bytes.
  std::vector<std::optional<int64_t>> estimatedIncrementalBytes(
      input->childrenSize());
  for (column_index_t column = 0; column < numColumns_; ++column) {
    const auto inputColumn = outputToInputChannel(column);
    if (estimatedIncrementalBytes[inputColumn].has_value()) {
      estimatedBytes += *estimatedIncrementalBytes[inputColumn];
      continue;
    }
    const auto& columnType = outputType_->childAt(column);
    if (columnType->isUnknown()) {
      VELOX_UNSUPPORTED(
          "Unsupported type kind for "
          "PrestoIterativePartitioningSerializer::estimateBytesAfterAppend: {}",
          columnType->kind());
    } else if (columnType->isFixedWidth()) {
      const auto* columnState = bufferState_->children()[column].get();
      const auto inputNulls = countNulls(*input->childAt(inputColumn));
      const auto partitionsWithNulls = std::min<uint32_t>(
          bufferState_->numNonEmptyPartitions() + numNewPartitions,
          columnState->numPartitionsWithNulls() + inputNulls.value_or(numRows));
      const auto nullBitmapBytes = maxBitmapBytes(
          bufferState_->rowsBuffered() + numRows, partitionsWithNulls);
      auto nullBitmapBytesBuffered = columnState->nullBitmapBytesBuffered();
      VELOX_DCHECK_GE(nullBitmapBytes, nullBitmapBytesBuffered);

      estimatedIncrementalBytes[inputColumn] = numNewPartitions *
              simpleColumnBytes(columnType, 0, 0) + // header growth
          nullBitmapBytes -
          nullBitmapBytesBuffered + // null bitmap growth
          static_cast<int64_t>(numRows - inputNulls.value_or(0)) *
              fixedTypeWidth(columnType->kind()); // value bytes growth
      estimatedBytes += *estimatedIncrementalBytes[inputColumn];
    } else {
      switch (columnType->kind()) {
        case TypeKind::VARCHAR:
        case TypeKind::VARBINARY:
        case TypeKind::ROW:
        case TypeKind::ARRAY:
        case TypeKind::MAP:
          VELOX_NYI(
              "Unsupported type kind for "
              "PrestoIterativePartitioningSerializer::estimateBytesAfterAppend: {}",
              columnType->kind());
        default:
          VELOX_UNSUPPORTED(
              "Unsupported type kind for "
              "PrestoIterativePartitioningSerializer::estimateBytesAfterAppend: {}",
              columnType->kind());
      }
    }
  }
  return estimatedBytes;
}

void PrestoIterativePartitioningSerializer::append(
    const RowVectorPtr& input,
    const std::vector<uint32_t>& partitions) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_EQ(
      input->size(),
      partitions.size(),
      "partitions.size() must equal input->size()");

  validateOutputInputMapping(input);

  if (input->size() == 0) {
    return;
  }

  PartitionBuildContext ctx;
  ctx.numVirtualPartitions = numVirtualPartitions_;
  auto partitionedRowVector = PartitionedVector::create(
      std::static_pointer_cast<BaseVector>(input),
      partitions,
      numPartitions_,
      ctx,
      pool_);

  bufferState_->append(partitionedRowVector, outputToInputChannels_);
  partitionedRowVectors_.push_back(std::move(partitionedRowVector));
}

// ---------------------------------------------------------------------------
// Top-level flush
// ---------------------------------------------------------------------------

std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
PrestoIterativePartitioningSerializer::flush() {
  auto pages =
      (opts_.compressionKind == common::CompressionKind::CompressionKind_NONE)
      ? flushUncompressed()
      : flushCompressed();

  clear();

  return pages;
}

std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
PrestoIterativePartitioningSerializer::flushUncompressed() {
  if (partitionedRowVectors_.empty()) {
    return {};
  }

  // 1. Determine non-empty partitions.
  std::vector<uint32_t> nonEmptyPartitions;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    if (bufferState_->rowsPerPartition()[p] > 0) {
      nonEmptyPartitions.push_back(p);
    }
  }
  const auto& rowSchema = outputType_->asRow();

  // 2. Create per-partition listeners first so the codec mask can be derived
  // from whether the factory actually produced a listener. The factory may
  // return nullptr (e.g. when OutputBufferManager has no listener factory
  // set), in which case checksumming is skipped and the checksum bit must not
  // be set in the codec byte.
  std::vector<std::unique_ptr<OutputStreamListener>> listeners(numPartitions_);
  for (uint32_t p : nonEmptyPartitions) {
    if (listenerFactory_) {
      listeners[p] = listenerFactory_();
    }
  }
  const bool checksumEnabled = !nonEmptyPartitions.empty() &&
      listeners[nonEmptyPartitions[0]] != nullptr;
  const char codecMask = getCodecMarker(checksumEnabled);

  // 3. Create output streams sized to the exact bytes each partition will need,
  // so that the entire payload fits. This avoids multiple resizing and copying.
  std::vector<std::unique_ptr<IOBufOutputStream>> outputStreams(numPartitions_);
  std::vector<IOBufOutputStream*> rawOutputStreams(numPartitions_);
  std::vector<std::streampos> beginStreamPositions(numPartitions_);

  for (uint32_t p : nonEmptyPartitions) {
    outputStreams[p] = std::make_unique<IOBufOutputStream>(
        *pool_, listeners[p].get(), bufferState_->bytesPerPartition()[p]);
    rawOutputStreams[p] = outputStreams[p].get();
    beginStreamPositions[p] = outputStreams[p]->tellp();

    flushStart(*outputStreams[p], p, codecMask);
  }

  // 4. Flush column data.
  flushRowChildren(
      partitionedRowVectors_, rowSchema, nonEmptyPartitions, rawOutputStreams);

  // 5. Finalize the page by seeking back to fill in sizes and CRC, and get the
  // IOBuf and numOfRows from each stream.
  std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
      result;
  for (uint32_t p : nonEmptyPartitions) {
    flushFinish(
        *outputStreams[p],
        p,
        beginStreamPositions[p],
        codecMask,
        listeners[p].get());
    result[p] = std::make_pair(
        outputStreams[p]->getIOBuf(), bufferState_->rowsPerPartition()[p]);
  }

  return result;
}

std::map<uint32_t, std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>
PrestoIterativePartitioningSerializer::flushCompressed() {
  VELOX_NYI();
}

// ---------------------------------------------------------------------------
// Second level functions: start, columns and finish
// ---------------------------------------------------------------------------

void PrestoIterativePartitioningSerializer::flushStart(
    IOBufOutputStream& out,
    uint32_t partition,
    char codecMask) const {
  auto* listener = dynamic_cast<PrestoOutputStreamListener*>(out.listener());
  if (listener) {
    listener->pause();
  }

  // Write 21-byte Presto page header; sizes and CRC are filled in later.
  const int32_t numRows =
      static_cast<int32_t>(bufferState_->rowsPerPartition()[partition]);
  char header[kHeaderSize] = {};
  std::memcpy(&header[0], &numRows, 4);
  std::memcpy(&header[4], &codecMask, 1);
  out.write(header, kHeaderSize);

  if (listener) {
    listener->resume();
  }

  // Number of columns is included in the CRC.
  const int32_t numCols = static_cast<int32_t>(numColumns_);
  out.write(reinterpret_cast<const char*>(&numCols), 4);
}

void PrestoIterativePartitioningSerializer::flushRowChildren(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const RowType& rowSchema,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  for (uint32_t col = 0; col < rowSchema.size(); ++col) {
    std::vector<PartitionedVectorPtr> column;
    column.reserve(partitionedVectors.size());
    for (const auto& partitionedVector : partitionedVectors) {
      const auto& partitionedRowVector =
          std::dynamic_pointer_cast<PartitionedRowVector>(partitionedVector);
      VELOX_DCHECK_NOT_NULL(partitionedRowVector.get());
      column.push_back(
          partitionedRowVector->childAt(outputToInputChannel(col)));
    }

    flushColumn(
        column, rowSchema.childAt(col), nonEmptyPartitions, outputStreams);
  }
}

void PrestoIterativePartitioningSerializer::flushFinish(
    IOBufOutputStream& out,
    uint32_t partition,
    std::streampos beginOffset,
    char codecMask,
    OutputStreamListener* listener) const {
  auto* prestoListener = dynamic_cast<PrestoOutputStreamListener*>(listener);
  if (prestoListener) {
    prestoListener->pause();
  }

  const std::streampos totalSize =
      static_cast<int32_t>(out.tellp() - beginOffset);
  const std::streampos uncompressedSize = totalSize - kHeaderSize;
  int64_t crc = 0;
  if (prestoListener) {
    crc = computeChecksum(
        *prestoListener,
        static_cast<int8_t>(codecMask),
        static_cast<int32_t>(bufferState_->rowsPerPartition()[partition]),
        uncompressedSize);
  }

  out.seekp(beginOffset + kUncompressedSizeOffset);
  writeInt32(&out, uncompressedSize);
  writeInt32(&out, uncompressedSize); // TODO: compressedSize
  writeInt64(&out, crc);
  out.seekp(beginOffset + totalSize);
}

// ---------------------------------------------------------------------------
// Column-level dispatch
// ---------------------------------------------------------------------------

void PrestoIterativePartitioningSerializer::flushColumn(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  VELOX_CHECK_GT(partitionedVectors.size(), 0);

  auto typeKind = partitionedVectors[0]->baseVector()->typeKind();
  switch (typeKind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::HUGEINT:
      flushSimpleColumn(
          partitionedVectors, colType, nonEmptyPartitions, outputStreams);
      break;

    case TypeKind::TIMESTAMP:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::ROW:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
      VELOX_NYI(
          "Unsupported vector type kind for PrestoIterativePartitioningSerializer: {}",
          typeKind);

    default:
      VELOX_UNSUPPORTED(
          "Invalid vector type kind for PrestoIterativePartitioningSerializer: {}",
          typeKind);
  }
}

void PrestoIterativePartitioningSerializer::flushSimpleColumn(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  flushHeader(typeToEncodingName(colType), nonEmptyPartitions, outputStreams);
  flushRowCounts(nonEmptyPartitions, outputStreams);
  flushNulls(partitionedVectors, nonEmptyPartitions, outputStreams);

  for (size_t i = 0; i < partitionedVectors.size(); i++) {
    flushSingleSimpleVector(partitionedVectors[i], outputStreams);
  }
}

template <TypeKind kind>
void PrestoIterativePartitioningSerializer::flushSingleFlatVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<T>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawValues =
      flatVector->baseVector()->template as<FlatVector<T>>()->rawValues();
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = flatVector->rawPartitionOffsets();

  flushFlatValues<T>(rawValues, rawNulls, partitionOffsets, outputStreams);
}

// BOOLEAN columns use kByteArray encoding: FlatVector<bool> stores bits
// packed, so rawValues() is unsupported. Each non-null value is written as
// one byte (0x00 or 0x01).
template <>
void PrestoIterativePartitioningSerializer::flushSingleFlatVector<
    TypeKind::BOOLEAN>(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<bool>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawBoolValues =
      flatVector->baseVector()->as<FlatVector<bool>>()->rawValues<uint64_t>();
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = flatVector->rawPartitionOffsets();

  // TODO: Improve performance
  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    const auto numValues = offset - lastOffset;
    const auto numNulls = partitionedVector->numNullsAt(p);
    if (outputStreams[p] != nullptr && numValues > 0) {
      if (numNulls == 0) {
        for (vector_size_t i = lastOffset; i < offset; ++i) {
          const int8_t val = bits::isBitSet(rawBoolValues, i) ? 1 : 0;
          outputStreams[p]->write(reinterpret_cast<const char*>(&val), 1);
        }
      } else {
        VELOX_DCHECK_NOT_NULL(rawNulls);
        for (vector_size_t i = lastOffset; i < offset; ++i) {
          if (!bits::isBitNull(rawNulls, i)) {
            const int8_t val = bits::isBitSet(rawBoolValues, i) ? 1 : 0;
            outputStreams[p]->write(reinterpret_cast<const char*>(&val), 1);
          }
        }
      }
    }
    lastOffset = offset;
  }
}

template <TypeKind kind>
void PrestoIterativePartitioningSerializer::flushSingleConstantVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  if constexpr (
      kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY ||
      kind == TypeKind::TIMESTAMP) {
    VELOX_NYI(
        "flushSingleConstantVector does not support variable-length type: {}",
        kind);
  }

  using T = typename TypeTraits<kind>::NativeType;
  auto* constantVector =
      partitionedVector->baseVector()->template as<ConstantVector<T>>();
  VELOX_DCHECK_NOT_NULL(constantVector);

  if (constantVector->isNullAt(0)) {
    return;
  }

  const auto value = constantVector->valueAtFast(0);
  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();

  Scratch scratch;
  ScratchPtr<T> values(scratch);
  const auto numRowsPerChunk =
      std::max<vector_size_t>(1, kChunkBytes / sizeof(T));
  const char* chunkBytes = nullptr;

  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    auto numRows = offset - lastOffset;
    if (numRows > 0) {
      VELOX_DCHECK_NOT_NULL(outputStreams[p]);

      if (chunkBytes == nullptr) {
        auto* ptr = values.get(numRowsPerChunk);
        std::fill_n(ptr, numRowsPerChunk, value);
        chunkBytes = reinterpret_cast<const char*>(ptr);
      }

      while (numRows > 0) {
        auto n = std::min<vector_size_t>(numRowsPerChunk, numRows);
        outputStreams[p]->write(chunkBytes, n * sizeof(T));
        numRows -= n;
      }
    }
    lastOffset = offset;
  }
}

void PrestoIterativePartitioningSerializer::flushSingleSimpleVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  auto encoding = partitionedVector->baseVector()->encoding();
  auto typeKind = partitionedVector->baseVector()->typeKind();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flushSingleFlatVector, typeKind, partitionedVector, outputStreams);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flushSingleConstantVector,
          typeKind,
          partitionedVector,
          outputStreams);
      break;
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      VELOX_NYI(
          "Unsupported vector encoding for PrestoIterativePartitioningSerializer: {}",
          encoding);
    default:
      VELOX_UNSUPPORTED(
          "Invalid vector encoding for PrestoIterativePartitioningSerializer:flushSingleSimpleVector: {}",
          encoding);
  }
}

// ---------------------------------------------------------------------------
// Column building blocks
// ---------------------------------------------------------------------------

void PrestoIterativePartitioningSerializer::flushHeader(
    std::string_view name,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  const int32_t nameLen = static_cast<int32_t>(name.size());
  for (uint32_t p : nonEmptyPartitions) {
    writeInt32(outputStreams[p], nameLen);
    outputStreams[p]->write(name.data(), nameLen);
  }
}

void PrestoIterativePartitioningSerializer::flushRowCounts(
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  for (uint32_t p : nonEmptyPartitions) {
    writeInt32(
        outputStreams[p],
        static_cast<int32_t>(bufferState_->rowsPerPartition()[p]));
  }
}

void PrestoIterativePartitioningSerializer::flushNulls(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  std::vector<vector_size_t> nullCounts(numPartitions_, 0);
  for (uint32_t p : nonEmptyPartitions) {
    for (const auto& pv : partitionedVectors) {
      nullCounts[p] += pv->numNullsAt(p);
    }
    const char flagByte = nullCounts[p] > 0 ? 1 : 0;
    outputStreams[p]->write(&flagByte, 1);
  }

  const bool hasAnyNulls = std::any_of(
      nonEmptyPartitions.begin(), nonEmptyPartitions.end(), [&](uint32_t p) {
        return nullCounts[p] > 0;
      });
  if (!hasAnyNulls) {
    return;
  }

  // Build each partition's null bitmap in a temporary buffer, accumulating
  // bits across all batches. Writing via write() correctly handles range
  // boundaries in the output stream without requiring seekp().
  // TODO: Avoid this extra memory allocation and copy
  std::vector<std::vector<uint8_t>> bitmaps(numPartitions_);
  for (uint32_t p : nonEmptyPartitions) {
    if (nullCounts[p] > 0) {
      bitmaps[p].assign(
          bits::nbytes(bufferState_->rowsPerPartition()[p]),
          bits::kNotNullByte);
    }
  }

  std::vector<vector_size_t> destBitOffsets(numPartitions_, 0);
  for (const auto& pv : partitionedVectors) {
    auto encoding = pv->baseVector()->encoding();
    switch (encoding) {
      case VectorEncoding::Simple::FLAT:
        flushSimpleVectorNulls(pv, nonEmptyPartitions, bitmaps, destBitOffsets);
        break;
      case VectorEncoding::Simple::CONSTANT:
        flushConstantVectorNulls(
            pv, nonEmptyPartitions, bitmaps, destBitOffsets);
        break;
      case VectorEncoding::Simple::BIASED:
      case VectorEncoding::Simple::DICTIONARY:
      case VectorEncoding::Simple::SEQUENCE:
        VELOX_NYI(
            "Unsupported vector encoding for PrestoIterativePartitioningSerializer: {}",
            encoding);
      default:
        VELOX_UNSUPPORTED(
            "Invalid vector encoding for PrestoIterativePartitioningSerializer: {}",
            encoding);
    }
  }

  for (uint32_t p : nonEmptyPartitions) {
    if (nullCounts[p] == 0) {
      continue;
    }

    // Convert Velox format (LSB-first, 1=not-null) to Presto wire format
    // (MSB-first, 1=null) in-place.
    const int32_t numBytes = bits::nbytes(bufferState_->rowsPerPartition()[p]);
    for (int32_t i = 0; i < numBytes; ++i) {
      bitmaps[p][i] = ~bitmaps[p][i];
      bits::reverseBits(&bitmaps[p][i], 1);
    }

    outputStreams[p]->write(
        reinterpret_cast<const char*>(bitmaps[p].data()), numBytes);
  }
}

void PrestoIterativePartitioningSerializer::flushSimpleVectorNulls(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<uint32_t>& nonEmptyPartitions,
    std::vector<std::vector<uint8_t>>& bitmaps,
    std::vector<vector_size_t>& destBitOffsets) {
  const uint64_t* rawNulls = partitionedVector->baseVector()->rawNulls();
  const auto* rawPartitionOffsets = partitionedVector->rawPartitionOffsets();
  vector_size_t startBit = 0;
  for (uint32_t p : nonEmptyPartitions) {
    vector_size_t numBits = rawPartitionOffsets[p] - startBit;
    if (rawNulls && numBits > 0 && !bitmaps[p].empty()) {
      bits::copyBits(
          rawNulls,
          startBit,
          reinterpret_cast<uint64_t*>(bitmaps[p].data()),
          destBitOffsets[p],
          numBits);
    }
    if (!bitmaps[p].empty()) {
      destBitOffsets[p] += numBits;
    }
    startBit = rawPartitionOffsets[p];
  }
}

void PrestoIterativePartitioningSerializer::flushConstantVectorNulls(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<uint32_t>& nonEmptyPartitions,
    std::vector<std::vector<uint8_t>>& bitmaps,
    std::vector<vector_size_t>& destBitOffsets) {
  const bool isNullConstant = partitionedVector->baseVector()->isNullAt(0);
  const auto* rawPartitionOffsets = partitionedVector->rawPartitionOffsets();
  vector_size_t startBit = 0;
  for (uint32_t p : nonEmptyPartitions) {
    vector_size_t numBits = rawPartitionOffsets[p] - startBit;
    if (isNullConstant && numBits > 0 && !bitmaps[p].empty()) {
      bits::fillBits(
          reinterpret_cast<uint64_t*>(bitmaps[p].data()),
          destBitOffsets[p],
          destBitOffsets[p] + numBits,
          bits::kNull);
    }
    if (!bitmaps[p].empty()) {
      destBitOffsets[p] += numBits;
    }
    startBit = rawPartitionOffsets[p];
  }
}

template <typename T>
void PrestoIterativePartitioningSerializer::flushFlatValues(
    const T* partitionedValues,
    const uint64_t* rawNulls,
    const vector_size_t* partitionOffsets,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  const auto typeWidth = sizeof(T);
  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    const auto numValues = offset - lastOffset;
    if (outputStreams[p] != nullptr && numValues > 0) {
      if (!rawNulls) {
        outputStreams[p]->write(
            reinterpret_cast<const char*>(&partitionedValues[lastOffset]),
            numValues * typeWidth);
      } else {
        // Presto writes only non-null values; null slots are omitted.
        // TODO: Improve performance
        for (vector_size_t i = lastOffset; i < offset; ++i) {
          if (!bits::isBitNull(rawNulls, i)) {
            outputStreams[p]->write(
                reinterpret_cast<const char*>(&partitionedValues[i]),
                typeWidth);
          }
        }
      }
    }
    lastOffset = offset;
  }
}

void PrestoIterativePartitioningSerializer::flushSequentialOffsets(
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  for (uint32_t p : nonEmptyPartitions) {
    const int32_t numRows =
        static_cast<int32_t>(bufferState_->rowsPerPartition()[p]);
    for (int32_t i = 0; i <= numRows; ++i) {
      writeInt32(outputStreams[p], i);
    }
  }
}

} // namespace facebook::velox::serializer::presto
