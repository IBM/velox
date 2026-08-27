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
#include "velox/common/base/Nulls.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DictionaryVector.h"
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
/// sizeof(WireType) used in flushValuesChunked.
int32_t fixedTypeWidth(TypeKind kind, const SerdeOpts& opts) {
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
    case TypeKind::HUGEINT:
      return 16;
    case TypeKind::TIMESTAMP:
      return opts.useLosslessTimestamp ? 16 : 8;
    default:
      return 0;
  }
}

/// Returns the exact bytes for one fixed-width column in one partition.
int64_t simpleColumnBytes(
    const TypePtr& colType,
    int64_t numRows,
    int64_t numNulls,
    int64_t valueWidth) {
  const auto encodingName = typeToEncodingName(colType);
  return 4 + static_cast<int64_t>(encodingName.size()) + // header
      4 + // rowCount
      1 + // nullFlag
      (numNulls > 0 ? bits::nbytes(numRows) : 0) + // null bitmap
      (numRows - numNulls) * valueWidth; // values
}

/// Returns the exact bytes for the framing of one ROW column block in one
/// partition: the encoding header, numFields, and the block footer. The child
/// column bytes are accounted separately by the child states.
int64_t rowColumnFramingBytes(int64_t numRows, int64_t numNulls) {
  return 4 + static_cast<int64_t>(kRow.size()) + // header
      4 + // numFields
      4 + // rowCount
      4 * (numRows + 1) + // offsets
      1 + // nullFlag
      (numNulls > 0 ? bits::nbytes(numRows) : 0); // null bitmap
}

int64_t variableWidthColumnBytes(
    int64_t numRows,
    int64_t numNulls,
    int64_t valueBytes) {
  return 4 + static_cast<int64_t>(kVariableWidth.size()) + // header
      4 + // rowCount
      4 * numRows + // offsets
      1 + // nullFlag
      (numNulls > 0 ? bits::nbytes(numRows) : 0) + // null bitmap
      4 + // valueBytes
      valueBytes; // values
}

int64_t variableWidthDataBytes(const BaseVector& vector) {
  switch (vector.encoding()) {
    case VectorEncoding::Simple::FLAT: {
      const auto* flatVector = vector.asFlatVector<StringView>();
      VELOX_DCHECK_NOT_NULL(flatVector);

      const auto* rawValues = flatVector->rawValues();
      const auto* rawNulls = vector.rawNulls();

      int64_t dataBytes = 0;
      if (!rawNulls) {
        for (vector_size_t i = 0; i < vector.size(); ++i) {
          dataBytes += rawValues[i].size();
        }
      } else {
        for (vector_size_t i = 0; i < vector.size(); ++i) {
          if (!bits::isBitNull(rawNulls, i)) {
            dataBytes += rawValues[i].size();
          }
        }
      }
      return dataBytes;
    }
    case VectorEncoding::Simple::CONSTANT: {
      const auto* constantVector = vector.as<ConstantVector<StringView>>();
      VELOX_DCHECK_NOT_NULL(constantVector);

      if (constantVector->isNullAt(0)) {
        return 0;
      }

      return static_cast<int64_t>(vector.size()) *
          constantVector->valueAt(0).size();
    }
    case VectorEncoding::Simple::DICTIONARY: {
      const auto* simpleVector = vector.as<SimpleVector<StringView>>();
      VELOX_DCHECK_NOT_NULL(simpleVector);

      int64_t dataBytes = 0;
      for (vector_size_t i = 0; i < vector.size(); ++i) {
        if (!simpleVector->isNullAt(i)) {
          dataBytes += simpleVector->valueAt(i).size();
        }
      }
      return dataBytes;
    }
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::SEQUENCE:
      VELOX_NYI(
          "Unsupported vector encoding for variable-width size estimation: {}",
          vector.encoding());
    default:
      VELOX_UNSUPPORTED(
          "Invalid vector encoding for variable-width size estimation: {}",
          vector.encoding());
  }
}

void accumulateVariableWidthOffsetsForFlatVector(
    const PartitionedVectorPtr& partitionedVector,
    std::vector<std::vector<int32_t>>& offsetsPerPartition) {
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<StringView>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawValues =
      flatVector->baseVector()->asFlatVector<StringView>()->rawValues();
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();

  vector_size_t lastPartitionOffset = 0;
  for (uint32_t p = 0; p < offsetsPerPartition.size(); ++p) {
    const auto partitionOffset = partitionOffsets[p];
    auto& offsets = offsetsPerPartition[p];
    int32_t endOffset = offsets.empty() ? 0 : offsets.back();
    if (!rawNulls) {
      for (auto i = lastPartitionOffset; i < partitionOffset; ++i) {
        endOffset += rawValues[i].size();
        offsets.push_back(endOffset);
      }
    } else {
      for (auto i = lastPartitionOffset; i < partitionOffset; ++i) {
        if (!bits::isBitNull(rawNulls, i)) {
          endOffset += rawValues[i].size();
        }
        offsets.push_back(endOffset);
      }
    }
    lastPartitionOffset = partitionOffset;
  }
}

void accumulateVariableWidthOffsetsForConstantVector(
    const PartitionedVectorPtr& partitionedVector,
    std::vector<std::vector<int32_t>>& offsetsPerPartition) {
  const auto* constantVector =
      partitionedVector->baseVector()->as<ConstantVector<StringView>>();
  VELOX_DCHECK_NOT_NULL(constantVector);

  const auto valueSize =
      constantVector->isNullAt(0) ? 0 : constantVector->valueAt(0).size();
  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();

  vector_size_t lastPartitionOffset = 0;
  for (uint32_t p = 0; p < offsetsPerPartition.size(); ++p) {
    const auto partitionOffset = partitionOffsets[p];
    auto& offsets = offsetsPerPartition[p];
    int32_t endOffset = offsets.empty() ? 0 : offsets.back();
    for (auto i = lastPartitionOffset; i < partitionOffset; ++i) {
      endOffset += valueSize;
      offsets.push_back(endOffset);
    }
    lastPartitionOffset = partitionOffset;
  }
}

void accumulateVariableWidthOffsetsForDictionaryVector(
    const PartitionedVectorPtr& partitionedVector,
    std::vector<std::vector<int32_t>>& offsetsPerPartition) {
  const auto* simpleVector =
      partitionedVector->baseVector()->as<SimpleVector<StringView>>();
  VELOX_DCHECK_NOT_NULL(simpleVector);

  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();

  vector_size_t lastPartitionOffset = 0;
  for (uint32_t p = 0; p < offsetsPerPartition.size(); ++p) {
    const auto partitionOffset = partitionOffsets[p];
    auto& offsets = offsetsPerPartition[p];
    int32_t endOffset = offsets.empty() ? 0 : offsets.back();
    for (auto i = lastPartitionOffset; i < partitionOffset; ++i) {
      if (!simpleVector->isNullAt(i)) {
        endOffset += simpleVector->valueAt(i).size();
      }
      offsets.push_back(endOffset);
    }
    lastPartitionOffset = partitionOffset;
  }
}

/// Accumulates per-partition end offsets for partitionedVector.
void accumulateVariableWidthOffsets(
    const PartitionedVectorPtr& partitionedVector,
    std::vector<std::vector<int32_t>>& offsetsPerPartition) {
  switch (partitionedVector->baseVector()->encoding()) {
    case VectorEncoding::Simple::FLAT:
      accumulateVariableWidthOffsetsForFlatVector(
          partitionedVector, offsetsPerPartition);
      break;
    case VectorEncoding::Simple::CONSTANT:
      accumulateVariableWidthOffsetsForConstantVector(
          partitionedVector, offsetsPerPartition);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      accumulateVariableWidthOffsetsForDictionaryVector(
          partitionedVector, offsetsPerPartition);
      break;
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::SEQUENCE:
      VELOX_NYI(
          "Unsupported vector encoding for variable-width offset accumulation: {}",
          partitionedVector->baseVector()->encoding());
    default:
      VELOX_UNSUPPORTED(
          "Invalid vector encoding for variable-width offset accumulation: {}",
          partitionedVector->baseVector()->encoding());
  }
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

// Reports a ROW column whose vector is not ROW-encoded. A struct-typed column
// can also arrive CONSTANT-encoded, which the ROW block writer does not handle
// yet.
[[noreturn]] void unsupportedRowEncoding(VectorEncoding::Simple encoding) {
  VELOX_NYI(
      "Unsupported encoding for a ROW column in "
      "PrestoIterativePartitioningSerializer: {}",
      encoding);
}

// Returns 'partitionedVector' as a PartitionedRowVector. Only a ROW-encoded
// vector yields one: PartitionedVector::create() builds a
// PartitionedConstantVector for a CONSTANT-encoded struct, so the encoding must
// be checked before the cast.
const PartitionedRowVector& asPartitionedRowVector(
    const PartitionedVectorPtr& partitionedVector) {
  const auto* rowVector =
      dynamic_cast<const PartitionedRowVector*>(partitionedVector.get());
  if (rowVector == nullptr) {
    unsupportedRowEncoding(partitionedVector->baseVector()->encoding());
  }
  return *rowVector;
}

// Materializes the nulls of 'vector' into one flat bitmap, resolving nulls
// added by wrapping encodings row by row. Returns nullptr when the vector has
// no nulls.
BufferPtr combinedNulls(const BaseVector& vector, memory::MemoryPool* pool) {
  if (!vector.mayHaveNulls()) {
    return nullptr;
  }

  auto nulls = AlignedBuffer::allocate<uint64_t>(
      bits::nwords(vector.size()), pool, bits::kNotNull64);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  for (vector_size_t row = 0; row < vector.size(); ++row) {
    if (vector.isNullAt(row)) {
      bits::setNull(rawNulls, row);
    }
  }

  return nulls;
}

// Returns the number of rows in [begin, end) that are live (the parent is not
// null) and not null themselves. Either bitmap may be nullptr, meaning all
// bits set.
vector_size_t countLiveRows(
    const uint64_t* rawNulls,
    const uint64_t* parentNulls,
    vector_size_t begin,
    vector_size_t end) {
  if (parentNulls != nullptr && rawNulls != nullptr) {
    vector_size_t count = 0;
    bits::forEachWord(begin, end, [&](int32_t index, uint64_t mask) {
      count +=
          __builtin_popcountll(parentNulls[index] & rawNulls[index] & mask);
    });
    return count;
  }
  if (parentNulls != nullptr) {
    return bits::countBits(parentNulls, begin, end);
  }
  if (rawNulls != nullptr) {
    return bits::countBits(rawNulls, begin, end);
  }
  return end - begin;
}

// Writes the live values of each partition of one column to its stream.
// A row is live when its parent (if any) is not null and the row itself is
// not null; dropped rows are omitted from the wire. 'valueAt' reads the wire
// value of one row. When every row of a partition is live and
// 'contiguousValues' is non-null the partition is written directly from it;
// otherwise values are gathered through 'valueAt' into a scratch chunk whose
// row count is precomputed.
template <typename WireType, typename ValueAt>
void flushValuesChunked(
    const WireType* contiguousValues,
    const uint64_t* rawNulls,
    const uint64_t* parentNulls,
    const vector_size_t* partitionOffsets,
    uint32_t numPartitions,
    Scratch& scratch,
    const std::vector<IOBufOutputStream*>& outputStreams,
    ValueAt valueAt) {
  constexpr auto typeWidth = sizeof(WireType);
  ScratchPtr<WireType> values(scratch);
  const auto numRowsPerChunk =
      std::max<vector_size_t>(1, kChunkBytes / typeWidth);
  WireType* chunk = nullptr;

  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions; ++p) {
    const auto offset = partitionOffsets[p];
    const auto numValues = offset - lastOffset;
    if (numValues > 0) {
      VELOX_CHECK_NOT_NULL(outputStreams[p]);

      const auto numLive =
          countLiveRows(rawNulls, parentNulls, lastOffset, offset);
      if (numLive == numValues && contiguousValues != nullptr) {
        outputStreams[p]->write(
            reinterpret_cast<const char*>(&contiguousValues[lastOffset]),
            numValues * typeWidth);
      } else if (numLive > 0) {
        if (chunk == nullptr) {
          chunk = values.get(numRowsPerChunk);
        }
        vector_size_t i = lastOffset;
        auto remaining = numLive;
        while (remaining > 0) {
          const auto chunkRows = std::min(numRowsPerChunk, remaining);
          if (numLive == numValues) {
            // No rows are dropped: read sequentially.
            for (vector_size_t j = 0; j < chunkRows; ++j) {
              chunk[j] = valueAt(i + j);
            }
            i += chunkRows;
          } else {
            vector_size_t count = 0;
            while (count < chunkRows) {
              const bool parentLive =
                  !parentNulls || bits::isBitSet(parentNulls, i);
              const bool rowIsNull = rawNulls && bits::isBitNull(rawNulls, i);
              if (parentLive && !rowIsNull) {
                chunk[count++] = valueAt(i);
              }
              ++i;
            }
          }
          outputStreams[p]->write(
              reinterpret_cast<const char*>(chunk), chunkRows * typeWidth);
          remaining -= chunkRows;
        }
      }
    }
    lastOffset = offset;
  }
}

// Two-word wire layout of one timestamp when useLosslessTimestamp is set.
struct LosslessTimestamp {
  int64_t seconds;
  int64_t nanos;
};

// Writes timestamp values through flushValuesChunked in the wire precision
// selected by 'opts'. 'valueAt' reads one row's Timestamp. The in-memory
// Timestamp layout never matches any wire layout, so there is no contiguous
// fast path.
template <typename ValueAt>
void flushTimestampValues(
    const SerdeOpts& opts,
    const uint64_t* rawNulls,
    const uint64_t* parentNulls,
    const vector_size_t* partitionOffsets,
    uint32_t numPartitions,
    Scratch& scratch,
    const std::vector<IOBufOutputStream*>& outputStreams,
    ValueAt valueAt) {
  if (opts.useLosslessTimestamp) {
    flushValuesChunked<LosslessTimestamp>(
        nullptr,
        rawNulls,
        parentNulls,
        partitionOffsets,
        numPartitions,
        scratch,
        outputStreams,
        [&](vector_size_t row) -> LosslessTimestamp {
          const Timestamp timestamp = valueAt(row);
          return {
              timestamp.getSeconds(),
              static_cast<int64_t>(timestamp.getNanos()),
          };
        });
  } else if (opts.useMicrosecondPrecision) {
    flushValuesChunked<int64_t>(
        nullptr,
        rawNulls,
        parentNulls,
        partitionOffsets,
        numPartitions,
        scratch,
        outputStreams,
        [&](vector_size_t row) { return valueAt(row).toMicros(); });
  } else {
    flushValuesChunked<int64_t>(
        nullptr,
        rawNulls,
        parentNulls,
        partitionOffsets,
        numPartitions,
        scratch,
        outputStreams,
        [&](vector_size_t row) { return valueAt(row).toMillis(); });
  }
}

// Writes one wire value repeated for every live row of each partition,
// chunked through scratch. 'parentLiveCounts' overrides the partition row
// counts when ancestor ROW levels drop rows.
template <typename WireType>
void flushConstantValue(
    const WireType& value,
    const vector_size_t* partitionOffsets,
    const std::vector<vector_size_t>* parentLiveCounts,
    uint32_t numPartitions,
    Scratch& scratch,
    const std::vector<IOBufOutputStream*>& outputStreams) {
  ScratchPtr<WireType> values(scratch);
  const auto numRowsPerChunk =
      std::max<vector_size_t>(1, kChunkBytes / sizeof(WireType));
  const char* chunkBytes = nullptr;

  // Every value of a non-null constant is written, so only the number of rows
  // to write per partition matters. 'parentLiveCounts' already excludes the
  // rows dropped by a null ancestor ROW level.
  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions; ++p) {
    const auto offset = partitionOffsets[p];
    auto numRows = parentLiveCounts != nullptr ? (*parentLiveCounts)[p]
                                               : offset - lastOffset;
    if (numRows > 0) {
      VELOX_DCHECK_NOT_NULL(outputStreams[p]);

      if (chunkBytes == nullptr) {
        auto* ptr = values.get(numRowsPerChunk);
        std::fill_n(ptr, numRowsPerChunk, value);
        chunkBytes = reinterpret_cast<const char*>(ptr);
      }

      while (numRows > 0) {
        const auto numChunkRows =
            std::min<vector_size_t>(numRowsPerChunk, numRows);
        outputStreams[p]->write(chunkBytes, numChunkRows * sizeof(WireType));
        numRows -= numChunkRows;
      }
    }
    lastOffset = offset;
  }
}

} // namespace

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

  static std::unique_ptr<ColumnBufferState>
  create(const TypePtr& type, uint32_t numPartitions, const SerdeOpts& opts);

  virtual void append(const PartitionedVectorPtr& partitionedVector) = 0;

  virtual int64_t estimateIncrementalBytes(
      const VectorPtr& input,
      uint32_t numNewPartitions) const = 0;

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

  vector_size_t rowsBuffered() const {
    vector_size_t total = 0;
    for (auto p = 0; p < numPartitions_; ++p) {
      total += rowsPerPartition_[p];
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
  FixedWidthBufferState(
      TypePtr type,
      uint32_t numPartitions,
      const SerdeOpts& opts)
      : ColumnBufferState(std::move(type), numPartitions),
        valueWidth_(fixedTypeWidth(type_->kind(), opts)) {}

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
      bytesPerPartition_[p] =
          simpleColumnBytes(type_, rows, nulls, valueWidth_);
    }
  }

  int64_t estimateIncrementalBytes(
      const VectorPtr& input,
      uint32_t numNewPartitions) const override {
    const auto numRows = input->size();
    const auto inputNulls = countNulls(*input);
    const auto partitionsWithNulls = std::min<uint32_t>(
        numNonEmptyPartitions_ + numNewPartitions,
        numPartitionsWithNulls_ + inputNulls.value_or(numRows));
    const auto nullBitmapBytes =
        maxBitmapBytes(rowsBuffered() + numRows, partitionsWithNulls);
    const auto bufferedNullBitmapBytes = nullBitmapBytesBuffered();
    VELOX_DCHECK_GE(nullBitmapBytes, bufferedNullBitmapBytes);

    return numNewPartitions *
        simpleColumnBytes(type_, 0, 0, valueWidth_) + // incremental header
        nullBitmapBytes -
        bufferedNullBitmapBytes + // incremental null bitmap bytes
        static_cast<int64_t>(numRows - inputNulls.value_or(0)) *
        valueWidth_; // incremental value bytes
  }

 private:
  // Serialized bytes per non-null value, fixed by the column type and serde
  // options at construction.
  const int64_t valueWidth_;
};

/// Buffer state for one VARCHAR or VARBINARY column.
class VariableWidthBufferState : public ColumnBufferState {
 public:
  VariableWidthBufferState(TypePtr type, uint32_t numPartitions)
      : ColumnBufferState(std::move(type), numPartitions),
        offsetsPerPartition_(numPartitions) {}

  void append(const PartitionedVectorPtr& partitionedVector) override {
    accumulateVariableWidthOffsets(partitionedVector, offsetsPerPartition_);

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

      const auto dataBytes = offsetsPerPartition_[p].empty()
          ? 0
          : static_cast<int64_t>(offsetsPerPartition_[p].back());
      bytesPerPartition_[p] = variableWidthColumnBytes(rows, nulls, dataBytes);
    }
  }

  int64_t estimateIncrementalBytes(
      const VectorPtr& input,
      uint32_t numNewPartitions) const override {
    const auto numRows = input->size();
    const auto inputNulls = countNulls(*input);
    const auto partitionsWithNulls = std::min<uint32_t>(
        numNonEmptyPartitions_ + numNewPartitions,
        numPartitionsWithNulls_ + inputNulls.value_or(numRows));
    const auto nullBitmapBytes =
        maxBitmapBytes(rowsBuffered() + numRows, partitionsWithNulls);
    const auto bufferedNullBitmapBytes = nullBitmapBytesBuffered();
    VELOX_DCHECK_GE(nullBitmapBytes, bufferedNullBitmapBytes);

    return numNewPartitions *
        variableWidthColumnBytes(0, 0, 0) + // incremental header bytes
        nullBitmapBytes -
        bufferedNullBitmapBytes + // incremental null bitmap bytes
        static_cast<int64_t>(numRows) * sizeof(int32_t) + // offsets
        variableWidthDataBytes(*input); // value bytes
  }

  const std::vector<int32_t>& offsetsAt(uint32_t partition) const {
    VELOX_DCHECK_LT(partition, numPartitions_);
    return offsetsPerPartition_[partition];
  }

  void clear() override {
    ColumnBufferState::clear();
    for (auto& offsets : offsetsPerPartition_) {
      offsets.clear();
    }
  }

 private:
  // Per-partition cumulative offsets for buffered variable-width rows.
  // Contains one offset per row, null rows keep the previous offset.
  // Keep offset value as int32_t to match the Presto wire format.
  // The upstream flush policy is expected to keep per-partition offsets within
  // the int32_t range.
  std::vector<std::vector<int32_t>> offsetsPerPartition_;
};

/// Buffer state for one ROW column.
///
/// Tracks this level's own rows and nulls and aggregates the sizes of the
/// child column states. Child row counts are upper bounds: a child state
/// counts every partitioned row, while serialization writes only the rows
/// whose ancestors are all non-null. The resulting byte counts are therefore
/// upper bounds too, which is what the output stream presizing needs.
class RowBufferState : public ColumnBufferState {
 public:
  RowBufferState(
      TypePtr type,
      uint32_t numPartitions,
      std::vector<std::unique_ptr<ColumnBufferState>> children)
      : ColumnBufferState(std::move(type), numPartitions),
        children_(std::move(children)) {}

  void append(const PartitionedVectorPtr& partitionedVector) override {
    const auto& rowVector = asPartitionedRowVector(partitionedVector);
    for (column_index_t column = 0; column < children_.size(); ++column) {
      children_[column]->append(rowVector.childAt(column));
    }

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

      int64_t bytes = rowColumnFramingBytes(rows, nulls);
      for (const auto& child : children_) {
        bytes += child->bytesPerPartition()[p];
      }
      bytesPerPartition_[p] = bytes;
    }
  }

  int64_t estimateIncrementalBytes(
      const VectorPtr& input,
      uint32_t numNewPartitions) const override {
    VectorPtr rewrittenInput = input;
    if (input->encoding() == VectorEncoding::Simple::DICTIONARY) {
      rewrittenInput = RowVector::pushDictionaryToRowVectorLeaves(input);
    }
    if (rewrittenInput->encoding() != VectorEncoding::Simple::ROW) {
      unsupportedRowEncoding(rewrittenInput->encoding());
    }

    auto rowVector = std::dynamic_pointer_cast<RowVector>(rewrittenInput);
    VELOX_DCHECK_NOT_NULL(rowVector);

    int64_t childBytes = 0;
    for (uint32_t col = 0; col < children_.size(); ++col) {
      childBytes += children_[col]->estimateIncrementalBytes(
          rowVector->childAt(col), numNewPartitions);
    }

    const auto numRows = rewrittenInput->size();
    const auto inputNulls = countNulls(*rewrittenInput);
    const auto partitionsWithNulls = std::min<uint32_t>(
        numNonEmptyPartitions_ + numNewPartitions,
        numPartitionsWithNulls_ + inputNulls.value_or(numRows));
    const auto nullBitmapBytes =
        maxBitmapBytes(rowsBuffered() + numRows, partitionsWithNulls);
    const auto bufferedNullBitmapBytes = nullBitmapBytesBuffered();
    VELOX_DCHECK_GE(nullBitmapBytes, bufferedNullBitmapBytes);

    return numNewPartitions *
        rowColumnFramingBytes(0, 0) + // incremental header bytes
        childBytes + // incremental child bytes
        4 * static_cast<int64_t>(numRows) + // incremental offsets bytes
        nullBitmapBytes -
        bufferedNullBitmapBytes; // incremental null bitmap bytes
  }

  void clear() override {
    ColumnBufferState::clear();
    for (auto& child : children_) {
      child->clear();
    }
  }

  const std::vector<std::unique_ptr<ColumnBufferState>>& children() const {
    return children_;
  }

 private:
  std::vector<std::unique_ptr<ColumnBufferState>> children_;
};

std::unique_ptr<ColumnBufferState> ColumnBufferState::create(
    const TypePtr& type,
    uint32_t numPartitions,
    const SerdeOpts& opts) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::HUGEINT:
    case TypeKind::TIMESTAMP:
      return std::make_unique<FixedWidthBufferState>(type, numPartitions, opts);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return std::make_unique<VariableWidthBufferState>(type, numPartitions);
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      std::vector<std::unique_ptr<ColumnBufferState>> children;
      children.reserve(rowType.size());
      for (auto column = 0; column < rowType.size(); ++column) {
        children.push_back(
            create(rowType.childAt(column), numPartitions, opts));
      }
      return std::make_unique<RowBufferState>(
          type, numPartitions, std::move(children));
    }
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

  static std::unique_ptr<BufferState>
  create(const RowTypePtr& type, uint32_t numPartitions, const SerdeOpts& opts);

  void append(
      const PartitionedVectorPtr& partitionedVector,
      const std::vector<column_index_t>& outputToInputChannels) {
    const auto& rowVector = asPartitionedRowVector(partitionedVector);

    rowsBuffered_ += partitionedVector->baseVector()->size();

    for (column_index_t column = 0; column < children_.size(); ++column) {
      const auto inputColumn = outputToInputChannels.empty()
          ? column
          : outputToInputChannels[column];
      children_[column]->append(rowVector.childAt(inputColumn));
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
    uint32_t numPartitions,
    const SerdeOpts& opts) {
  std::vector<std::unique_ptr<ColumnBufferState>> children;
  children.reserve(type->size());
  for (auto column = 0; column < type->size(); ++column) {
    children.push_back(
        ColumnBufferState::create(type->childAt(column), numPartitions, opts));
  }
  return std::make_unique<BufferState>(numPartitions, std::move(children));
}

PrestoIterativePartitioningSerializer::PrestoIterativePartitioningSerializer(
    RowTypePtr outputType,
    uint32_t numPartitions,
    const SerdeOpts& opts,
    memory::MemoryPool* pool,
    std::vector<column_index_t> outputToInputChannels,
    std::function<std::unique_ptr<OutputStreamListener>()> listenerFactory)
    : outputType_(std::move(outputType)),
      outputToInputChannels_(std::move(outputToInputChannels)),
      numPartitions_(numPartitions),
      opts_(opts),
      pool_(pool),
      listenerFactory_(std::move(listenerFactory)),
      numColumns_(outputType_->size()),
      bufferState_(BufferState::create(outputType_, numPartitions_, opts_)) {
  VELOX_CHECK_GT(numPartitions_, 0);
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK(
      outputToInputChannels_.empty() ||
          outputToInputChannels_.size() == outputType_->size(),
      "outputToInputChannels size must match output column count");
  VELOX_CHECK(
      !(opts_.useLosslessTimestamp && opts_.useMicrosecondPrecision),
      "useLosslessTimestamp and useMicrosecondPrecision are mutually exclusive");
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
    } else {
      estimatedIncrementalBytes[inputColumn] =
          bufferState_->children()[column]->estimateIncrementalBytes(
              input->childAt(inputColumn), numNewPartitions);
      estimatedBytes += *estimatedIncrementalBytes[inputColumn];
    }
  }
  return estimatedBytes;
}

void PrestoIterativePartitioningSerializer::append(
    const RowVectorPtr& input,
    const std::vector<uint32_t>& partitions) {
  VELOX_DCHECK_NOT_NULL(input);
  VELOX_DCHECK_EQ(
      input->size(),
      partitions.size(),
      "partitions.size() must equal input->size()");
  validateOutputInputMapping(input);

  if (input->size() == 0) {
    return;
  }

  PartitionBuildContext ctx;
  auto partitionedRowVector = PartitionedVector::create(
      std::static_pointer_cast<BaseVector>(input),
      partitions,
      numPartitions_,
      ctx,
      pool_);

  bufferState_->append(partitionedRowVector, outputToInputChannels_);
  partitionedRowVectors_.push_back(std::move(partitionedRowVector));
}

void PrestoIterativePartitioningSerializer::append(
    const RowVectorPtr& input,
    uint32_t singlePartition) {
  VELOX_DCHECK_NOT_NULL(input);
  VELOX_DCHECK_LT(singlePartition, numPartitions_);
  validateOutputInputMapping(input);

  if (input->size() == 0) {
    return;
  }

  PartitionBuildContext ctx;
  auto partitionedRowVector = PartitionedVector::create(
      std::static_pointer_cast<BaseVector>(input),
      singlePartition,
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

  // 4. Flush column data. Top-level columns have no parent nulls; row counts
  // come from the accumulated page state.
  SerializerContext context;
  context.rowCounts = bufferState_->rowsPerPartition();
  context.parentNulls.resize(partitionedRowVectors_.size());
  context.parentLiveCounts.resize(partitionedRowVectors_.size());
  flushRowChildren(
      partitionedRowVectors_,
      rowSchema,
      nonEmptyPartitions,
      rawOutputStreams,
      context);

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
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  for (uint32_t col = 0; col < rowSchema.size(); ++col) {
    std::vector<PartitionedVectorPtr> column;
    column.reserve(partitionedVectors.size());
    for (const auto& partitionedVector : partitionedVectors) {
      column.push_back(asPartitionedRowVector(partitionedVector)
                           .childAt(outputToInputChannel(col)));
    }

    const auto& columnState = *bufferState_->children()[col];
    flushColumn(
        columnState,
        column,
        rowSchema.childAt(col),
        nonEmptyPartitions,
        outputStreams,
        context);
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
    const ColumnBufferState& columnState,
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
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
    case TypeKind::TIMESTAMP:
      flushSimpleColumn(
          partitionedVectors,
          colType,
          nonEmptyPartitions,
          outputStreams,
          context);
      break;

    case TypeKind::ROW:
      flushRowColumn(
          columnState,
          partitionedVectors,
          colType,
          nonEmptyPartitions,
          outputStreams,
          context);
      break;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      flushVariableWidthColumn(
          columnState,
          partitionedVectors,
          colType,
          nonEmptyPartitions,
          outputStreams,
          context);
      break;

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

namespace {

// Appends the low 'count' bits of 'value' (count <= 64) to 'target' starting
// at bit 'bitOffset'. 'target' must be zero-initialized over the written range
// and own one extra addressable word past the last written bit.
inline void appendLowBits(
    uint64_t* target,
    uint64_t bitOffset,
    uint64_t value,
    uint32_t count) {
  const uint64_t word = bitOffset >> 6;
  const uint32_t shift = static_cast<uint32_t>(bitOffset & 63);
  target[word] |= value << shift;
  if (shift + count > 64) {
    target[word + 1] |= value >> (64 - shift);
  }
}

// Gathers the bits of 'source' at the positions in [begin, end) where 'mask'
// is set (every position when 'mask' is nullptr) and appends them, preserving
// order, to 'target' starting at bit 'targetBitOffset'. A nullptr 'source' is
// treated as all bits set (every selected row is not null). Returns the number
// of bits appended. The general case processes one 64-bit word at a time using
// bits::extractBits (parallel bit extract), so there is no per-row branching.
// 'target' must be zeroed over the written range with one extra addressable
// word past the last written bit.
int32_t compactBits(
    const uint64_t* source,
    const uint64_t* mask,
    int32_t begin,
    int32_t end,
    uint64_t* target,
    uint64_t targetBitOffset) {
  const int32_t numBits = end - begin;

  // Nothing is dropped, so the source bits move over unchanged.
  if (mask == nullptr) {
    if (source == nullptr) {
      bits::fillBits(
          target,
          static_cast<int32_t>(targetBitOffset),
          static_cast<int32_t>(targetBitOffset) + numBits,
          bits::kNotNull);
    } else {
      bits::copyBits(source, begin, target, targetBitOffset, numBits);
    }
    return numBits;
  }

  // Every selected row is not null, so only the number of selected rows
  // matters.
  if (source == nullptr) {
    const int32_t numSelected = bits::countBits(mask, begin, end);
    bits::fillBits(
        target,
        static_cast<int32_t>(targetBitOffset),
        static_cast<int32_t>(targetBitOffset) + numSelected,
        bits::kNotNull);
    return numSelected;
  }

  uint64_t outBit = targetBitOffset;
  bits::forEachWord(begin, end, [&](int32_t index, uint64_t wordMask) {
    const uint64_t selected = mask[index] & wordMask;
    if (selected == 0) {
      return;
    }
    const uint64_t packed =
        bits::extractBits<uint64_t>(source[index], selected);
    const uint32_t count = __builtin_popcountll(selected);
    appendLowBits(target, outBit, packed, count);
    outBit += count;
  });
  return static_cast<int32_t>(outBit - targetBitOffset);
}

// Returns the raw liveness mask of batch 'index', or nullptr when there is no
// mask, which means every row of the batch is live. An absent buffer is the
// canonical "no nulls at or above this level" marker, so callers do not need a
// separate flag.
const uint64_t* rawNullsAt(const std::vector<BufferPtr>& nulls, size_t index) {
  return nulls[index] ? nulls[index]->as<uint64_t>() : nullptr;
}

// Returns the live row counts per partition of batch 'index', or nullptr when
// every row of the batch is live and the partition's full row range must be
// used instead.
const std::vector<vector_size_t>* liveCountsAt(
    const std::vector<std::vector<vector_size_t>>& liveCounts,
    size_t index) {
  return liveCounts[index].empty() ? nullptr : &liveCounts[index];
}

} // namespace

void PrestoIterativePartitioningSerializer::flushSimpleColumn(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  flushHeader(typeToEncodingName(colType), nonEmptyPartitions, outputStreams);
  flushRowCounts(nonEmptyPartitions, outputStreams, context);
  flushNulls(partitionedVectors, nonEmptyPartitions, outputStreams, context);

  for (size_t i = 0; i < partitionedVectors.size(); i++) {
    flushSingleSimpleVector(
        partitionedVectors[i],
        outputStreams,
        rawNullsAt(context.parentNulls, i),
        liveCountsAt(context.parentLiveCounts, i));
  }
}

void PrestoIterativePartitioningSerializer::flushRowColumn(
    const ColumnBufferState& columnState,
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  const auto& rowState = dynamic_cast<const RowBufferState&>(columnState);
  const auto& rowSchema = colType->asRow();
  const int32_t numFields = static_cast<int32_t>(rowSchema.size());
  const size_t numVectors = partitionedVectors.size();

  // Number of parent-live rows that are null at this ROW level, per partition.
  std::vector<vector_size_t> nullCounts(numPartitions_, 0);

  SerializerContext childContext;
  childContext.rowCounts.assign(numPartitions_, 0);
  childContext.parentNulls.resize(numVectors);
  childContext.parentLiveCounts.resize(numVectors);

  // Step 1 + 2. For every batch, combine the incoming parentNulls with this
  // level's own nulls into the mask of rows that are live for the children
  // (parent-live and not null here), then count live and null rows per
  // partition with bits::countBits.
  for (size_t vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex) {
    const auto& partitionedVector = partitionedVectors[vectorIndex];
    auto baseVector = partitionedVector->baseVector();
    const vector_size_t numRows = baseVector->size();
    const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();
    const auto* parentNulls = rawNullsAt(context.parentNulls, vectorIndex);

    // A materialized but all-not-null bitmap drops no row, so it must not be
    // treated as "has nulls": propagating a mask that keeps every row would
    // push the children onto their row-at-a-time paths and would trip the
    // variable-width restriction below for no reason.
    const bool hasOwnNulls = baseVector->rawNulls() != nullptr &&
        BaseVector::countNulls(baseVector->nulls(), numRows) > 0;

    // The children see the rows that survive this level: parentNulls AND this
    // level's own nulls. Only the case where both drop rows needs a buffer of
    // its own; otherwise the surviving side's buffer is shared read-only. The
    // input vector's nulls are never modified — callers may still be holding
    // it. When nothing is dropped at or above this level the mask stays empty
    // (nullptr) and children treat every row as live.
    BufferPtr nulls;
    if (!hasOwnNulls) {
      nulls = context.parentNulls[vectorIndex];
    } else if (parentNulls == nullptr) {
      nulls = baseVector->nulls();
    } else {
      nulls = AlignedBuffer::allocate<uint64_t>(
          bits::nwords(numRows), pool_, /*value=*/0);
      bits::andBits(
          nulls->asMutable<uint64_t>(),
          baseVector->rawNulls(),
          parentNulls,
          0,
          numRows);
    }
    const auto* childLive = nulls ? nulls->as<uint64_t>() : nullptr;
    childContext.parentNulls[vectorIndex] = nulls;

    // The children only need explicit live counts when this batch drops rows.
    auto& liveCounts = childContext.parentLiveCounts[vectorIndex];
    if (childLive != nullptr) {
      liveCounts.assign(numPartitions_, 0);
    }

    for (uint32_t p : nonEmptyPartitions) {
      const vector_size_t begin = p == 0 ? 0 : partitionOffsets[p - 1];
      const vector_size_t end = partitionOffsets[p];
      if (end == begin) {
        continue;
      }

      const vector_size_t parentLive = parentNulls != nullptr
          ? bits::countBits(parentNulls, begin, end)
          : end - begin;
      const vector_size_t live = childLive != nullptr
          ? bits::countBits(childLive, begin, end)
          : end - begin;
      if (!liveCounts.empty()) {
        liveCounts[p] = live;
      }
      childContext.rowCounts[p] += live;
      nullCounts[p] += parentLive - live;
    }
  }

  // Header: "ROW" encoding name + numFields.
  flushHeader(kRow, nonEmptyPartitions, outputStreams);
  for (uint32_t p : nonEmptyPartitions) {
    writeInt32(outputStreams[p], numFields);
  }

  // Recurse into each child column with the propagated parent-null context.
  for (uint32_t col = 0; col < static_cast<uint32_t>(numFields); ++col) {
    std::vector<PartitionedVectorPtr> childVectors;
    childVectors.reserve(numVectors);
    for (const auto& partitionedVector : partitionedVectors) {
      childVectors.push_back(
          asPartitionedRowVector(partitionedVector).childAt(col));
    }
    flushColumn(
        *rowState.children()[col],
        childVectors,
        rowSchema.childAt(col),
        nonEmptyPartitions,
        outputStreams,
        childContext);
  }

  // Step 3. Footer. The number of rows at this level equals the number of
  // parent-live rows, which the parent recorded in context.rowCounts. Only
  // partitions that have nulls at this level need a compacted bitmap; the
  // rest use sequential offsets and no null section.
  std::vector<BufferPtr> nullsByPartition(numPartitions_);
  std::vector<uint64_t*> rawNullsByPartition(numPartitions_, nullptr);
  std::vector<uint64_t> nullsPartitionOffsets(numPartitions_, 0);
  for (uint32_t p : nonEmptyPartitions) {
    if (nullCounts[p] > 0) {
      const auto numWords = bits::nwords(context.rowCounts[p]) + 1;
      nullsByPartition[p] =
          AlignedBuffer::allocate<uint64_t>(numWords, pool_, 0);
      rawNullsByPartition[p] = nullsByPartition[p]->asMutable<uint64_t>();
    }
  }

  // Compact this level's live bits into each partition's bitmap, in batch
  // order, keeping only the positions where the parent is live.
  for (size_t vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex) {
    const auto* partitionOffsets =
        partitionedVectors[vectorIndex]->rawPartitionOffsets();
    const auto* parentNulls = rawNullsAt(context.parentNulls, vectorIndex);
    const auto* childLive = rawNullsAt(childContext.parentNulls, vectorIndex);

    for (uint32_t p : nonEmptyPartitions) {
      const vector_size_t begin = p == 0 ? 0 : partitionOffsets[p - 1];
      const vector_size_t end = partitionOffsets[p];
      if (rawNullsByPartition[p] == nullptr || end == begin) {
        continue;
      }
      nullsPartitionOffsets[p] += compactBits(
          childLive,
          parentNulls,
          begin,
          end,
          rawNullsByPartition[p],
          nullsPartitionOffsets[p]);
    }
  }

  flushRowCounts(nonEmptyPartitions, outputStreams, context);
  flushRowOffsets(
      nonEmptyPartitions,
      outputStreams,
      context.rowCounts,
      nullCounts,
      rawNullsByPartition);
  flushNullSection(
      nonEmptyPartitions,
      outputStreams,
      context.rowCounts,
      nullCounts,
      rawNullsByPartition);
}

void PrestoIterativePartitioningSerializer::flushVariableWidthColumn(
    const ColumnBufferState& columnState,
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const TypePtr& colType,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  // The buffered offsets and values cover every partitioned row, so a
  // variable-width column can only be serialized when no ancestor ROW drops
  // rows. A parent mask is only present when rows are actually dropped, which
  // this path does not handle yet.
  const bool dropsRows = std::any_of(
      context.parentNulls.begin(),
      context.parentNulls.end(),
      [](const BufferPtr& nulls) { return nulls != nullptr; });
  if (dropsRows) {
    VELOX_NYI(
        "Variable-width columns nested under a ROW with nulls are not supported "
        "by PrestoIterativePartitioningSerializer yet: {}",
        colType->toString());
  }

  flushHeader(typeToEncodingName(colType), nonEmptyPartitions, outputStreams);
  flushRowCounts(nonEmptyPartitions, outputStreams, context);
  flushOffsets(columnState, nonEmptyPartitions, outputStreams);
  flushNulls(partitionedVectors, nonEmptyPartitions, outputStreams, context);

  const auto* variableWidthState =
      dynamic_cast<const VariableWidthBufferState*>(&columnState);
  VELOX_DCHECK_NOT_NULL(variableWidthState);

  for (auto p : nonEmptyPartitions) {
    const auto& offsets = variableWidthState->offsetsAt(p);
    writeInt32(outputStreams[p], offsets.empty() ? 0 : offsets.back());
  }

  for (const auto& partitionedVector : partitionedVectors) {
    flushSingleVariableWidthVector(partitionedVector, outputStreams);
  }
}

template <TypeKind kind>
void PrestoIterativePartitioningSerializer::flushSingleFlatVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls) const {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<T>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawValues =
      flatVector->baseVector()->template as<FlatVector<T>>()->rawValues();
  // rawNulls() may be nullptr when the column has no nulls. Do not use
  // mutableRawNulls() here: it would materialize an all-not-null buffer and
  // mask the "no nulls" fast path.
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = flatVector->rawPartitionOffsets();

  flushValuesChunked(
      rawValues,
      rawNulls,
      parentNulls,
      partitionOffsets,
      numPartitions_,
      scratch_,
      outputStreams,
      [rawValues](vector_size_t row) { return rawValues[row]; });
}

// BOOLEAN columns use kByteArray encoding: FlatVector<bool> stores bits
// packed, so rawValues() is unsupported and there is no contiguous byte
// array to write from. Each non-null value is expanded to one byte
// (0x00 or 0x01).
template <>
void PrestoIterativePartitioningSerializer::flushSingleFlatVector<
    TypeKind::BOOLEAN>(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls) const {
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<bool>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawBoolValues =
      flatVector->baseVector()->as<FlatVector<bool>>()->rawValues<uint64_t>();
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = flatVector->rawPartitionOffsets();

  flushValuesChunked<int8_t>(
      nullptr,
      rawNulls,
      parentNulls,
      partitionOffsets,
      numPartitions_,
      scratch_,
      outputStreams,
      [rawBoolValues](vector_size_t row) -> int8_t {
        return bits::isBitSet(rawBoolValues, row) ? 1 : 0;
      });
}

template <>
void PrestoIterativePartitioningSerializer::flushSingleFlatVector<
    TypeKind::TIMESTAMP>(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls) const {
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<Timestamp>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawValues =
      flatVector->baseVector()->as<FlatVector<Timestamp>>()->rawValues();
  const auto* rawNulls = flatVector->baseVector()->rawNulls();

  flushTimestampValues(
      opts_,
      rawNulls,
      parentNulls,
      flatVector->rawPartitionOffsets(),
      numPartitions_,
      scratch_,
      outputStreams,
      [rawValues](vector_size_t row) { return rawValues[row]; });
}

template <TypeKind kind>
void PrestoIterativePartitioningSerializer::flushSingleConstantVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const std::vector<vector_size_t>* parentLiveCounts) const {
  if constexpr (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
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

  flushConstantValue(
      constantVector->valueAtFast(0),
      partitionedVector->rawPartitionOffsets(),
      parentLiveCounts,
      numPartitions_,
      scratch_,
      outputStreams);
}

template <>
void PrestoIterativePartitioningSerializer::flushSingleConstantVector<
    TypeKind::TIMESTAMP>(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const std::vector<vector_size_t>* parentLiveCounts) const {
  auto* constantVector =
      partitionedVector->baseVector()->as<ConstantVector<Timestamp>>();
  VELOX_DCHECK_NOT_NULL(constantVector);

  if (constantVector->isNullAt(0)) {
    return;
  }

  const auto value = constantVector->valueAtFast(0);
  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();
  if (opts_.useLosslessTimestamp) {
    flushConstantValue(
        LosslessTimestamp{
            value.getSeconds(),
            static_cast<int64_t>(value.getNanos()),
        },
        partitionOffsets,
        parentLiveCounts,
        numPartitions_,
        scratch_,
        outputStreams);
  } else if (opts_.useMicrosecondPrecision) {
    flushConstantValue(
        value.toMicros(),
        partitionOffsets,
        parentLiveCounts,
        numPartitions_,
        scratch_,
        outputStreams);
  } else {
    flushConstantValue(
        value.toMillis(),
        partitionOffsets,
        parentLiveCounts,
        numPartitions_,
        scratch_,
        outputStreams);
  }
}

template <TypeKind kind>
void PrestoIterativePartitioningSerializer::flushSingleDictionaryVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls) const {
  using T = typename TypeTraits<kind>::NativeType;
  // Booleans are stored as packed bits but written as one byte per value on
  // the wire; the implicit bool-to-int8_t conversion yields the 0/1 bytes.
  using WireType = std::conditional_t<kind == TypeKind::BOOLEAN, int8_t, T>;
  auto baseVector = partitionedVector->baseVector();
  auto* dictionaryVector = baseVector->as<DictionaryVector<T>>();
  VELOX_DCHECK_NOT_NULL(dictionaryVector);

  // Evaluate the dictionary's nulls as one flat bitmap so the shared chunked
  // writer applies. When the wrapped values have no nulls the wrapper's own
  // bitmap is that bitmap; otherwise the combined nulls are materialized row
  // by row.
  const auto* rawNulls = baseVector->rawNulls();
  BufferPtr combined;
  if (dictionaryVector->valueVector()->mayHaveNulls()) {
    combined = combinedNulls(*baseVector, pool_);
    rawNulls = combined ? combined->as<uint64_t>() : nullptr;
  }

  flushValuesChunked<WireType>(
      nullptr,
      rawNulls,
      parentNulls,
      partitionedVector->rawPartitionOffsets(),
      numPartitions_,
      scratch_,
      outputStreams,
      [dictionaryVector](vector_size_t row) -> WireType {
        return dictionaryVector->valueAtFast(row);
      });
}

template <>
void PrestoIterativePartitioningSerializer::flushSingleDictionaryVector<
    TypeKind::TIMESTAMP>(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls) const {
  auto baseVector = partitionedVector->baseVector();
  auto* dictionaryVector = baseVector->as<DictionaryVector<Timestamp>>();
  VELOX_DCHECK_NOT_NULL(dictionaryVector);

  // Evaluate the dictionary's nulls as one flat bitmap so the shared chunked
  // writer applies. When the wrapped values have no nulls the wrapper's own
  // bitmap is that bitmap; otherwise the combined nulls are materialized row
  // by row.
  const auto* rawNulls = baseVector->rawNulls();
  BufferPtr combined;
  if (dictionaryVector->valueVector()->mayHaveNulls()) {
    combined = combinedNulls(*baseVector, pool_);
    rawNulls = combined ? combined->as<uint64_t>() : nullptr;
  }

  flushTimestampValues(
      opts_,
      rawNulls,
      parentNulls,
      partitionedVector->rawPartitionOffsets(),
      numPartitions_,
      scratch_,
      outputStreams,
      [dictionaryVector](vector_size_t row) {
        return dictionaryVector->valueAtFast(row);
      });
}

void PrestoIterativePartitioningSerializer::flushSingleSimpleVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const uint64_t* parentNulls,
    const std::vector<vector_size_t>* parentLiveCounts) const {
  auto encoding = partitionedVector->baseVector()->encoding();
  auto typeKind = partitionedVector->baseVector()->typeKind();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flushSingleFlatVector,
          typeKind,
          partitionedVector,
          outputStreams,
          parentNulls);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flushSingleConstantVector,
          typeKind,
          partitionedVector,
          outputStreams,
          parentLiveCounts);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flushSingleDictionaryVector,
          typeKind,
          partitionedVector,
          outputStreams,
          parentNulls);
      break;
    case VectorEncoding::Simple::BIASED:
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

void PrestoIterativePartitioningSerializer::flushSingleVariableWidthFlatVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  auto* flatVector = partitionedVector->as<PartitionedFlatVector<StringView>>();
  VELOX_DCHECK_NOT_NULL(flatVector);

  const auto* rawValues =
      flatVector->baseVector()->as<FlatVector<StringView>>()->rawValues();
  const auto* rawNulls = flatVector->baseVector()->rawNulls();
  const auto* partitionOffsets = flatVector->rawPartitionOffsets();

  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    if (outputStreams[p] != nullptr) {
      if (!rawNulls) {
        for (auto i = lastOffset; i < offset; ++i) {
          outputStreams[p]->write(rawValues[i].data(), rawValues[i].size());
        }
      } else {
        for (auto i = lastOffset; i < offset; ++i) {
          if (!bits::isBitNull(rawNulls, i)) {
            outputStreams[p]->write(rawValues[i].data(), rawValues[i].size());
          }
        }
      }
    }
    lastOffset = offset;
  }
}

void PrestoIterativePartitioningSerializer::
    flushSingleVariableWidthConstantVector(
        const PartitionedVectorPtr& partitionedVector,
        const std::vector<IOBufOutputStream*>& outputStreams) const {
  const auto* constantVector =
      partitionedVector->baseVector()->as<ConstantVector<StringView>>();
  VELOX_DCHECK_NOT_NULL(constantVector);

  if (constantVector->isNullAt(0)) {
    return;
  }

  const auto value = constantVector->valueAt(0);
  const auto valueSize = value.size();
  if (valueSize == 0) {
    return;
  }

  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();

  const auto numRowsPerChunk =
      std::max<vector_size_t>(1, kChunkBytes / valueSize);
  const char* chunkBytes = value.data();
  ScratchPtr<char> chunk(scratch_);
  if (numRowsPerChunk > 1) {
    auto* ptr = chunk.get(numRowsPerChunk * valueSize);
    for (vector_size_t i = 0; i < numRowsPerChunk; ++i) {
      simd::memcpy(ptr + i * valueSize, value.data(), valueSize);
    }
    chunkBytes = ptr;
  }

  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const auto offset = partitionOffsets[p];
    auto numRows = offset - lastOffset;
    if (numRows > 0) {
      VELOX_DCHECK_NOT_NULL(outputStreams[p]);
      while (numRows > 0) {
        const auto n = std::min<vector_size_t>(numRowsPerChunk, numRows);
        outputStreams[p]->write(chunkBytes, n * valueSize);
        numRows -= n;
      }
    }
    lastOffset = offset;
  }
}

namespace {

// Writes the non-null variable-width values of one partitioned
// dictionary-encoded vector to each partition's stream in row order.
void flushSingleVariableWidthDictionaryVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams,
    uint32_t numPartitions) {
  const auto* simpleVector =
      partitionedVector->baseVector()->as<SimpleVector<StringView>>();
  VELOX_DCHECK_NOT_NULL(simpleVector);

  const auto* partitionOffsets = partitionedVector->rawPartitionOffsets();

  vector_size_t lastOffset = 0;
  for (uint32_t p = 0; p < numPartitions; ++p) {
    const auto offset = partitionOffsets[p];
    if (outputStreams[p] != nullptr && offset > lastOffset) {
      for (vector_size_t i = lastOffset; i < offset; ++i) {
        if (!simpleVector->isNullAt(i)) {
          const auto value = simpleVector->valueAt(i);
          outputStreams[p]->write(value.data(), value.size());
        }
      }
    }
    lastOffset = offset;
  }
}

} // namespace

void PrestoIterativePartitioningSerializer::flushSingleVariableWidthVector(
    const PartitionedVectorPtr& partitionedVector,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  const auto encoding = partitionedVector->baseVector()->encoding();

  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
      flushSingleVariableWidthFlatVector(partitionedVector, outputStreams);
      break;
    case VectorEncoding::Simple::CONSTANT:
      flushSingleVariableWidthConstantVector(partitionedVector, outputStreams);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      flushSingleVariableWidthDictionaryVector(
          partitionedVector, outputStreams, numPartitions_);
      break;
    case VectorEncoding::Simple::BIASED:
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
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  for (uint32_t p : nonEmptyPartitions) {
    writeInt32(outputStreams[p], static_cast<int32_t>(context.rowCounts[p]));
  }
}

void PrestoIterativePartitioningSerializer::flushNulls(
    const std::vector<PartitionedVectorPtr>& partitionedVectors,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const SerializerContext& context) const {
  const size_t numVectors = partitionedVectors.size();

  // Per-partition null bitmap accumulated across all batches, in Velox format
  // (1 == not null). One extra word so the bit appender in compactBits can
  // always touch the word past the last bit.
  std::vector<vector_size_t> nullCounts(numPartitions_, 0);
  std::vector<BufferPtr> nulls(numPartitions_);
  std::vector<uint64_t*> rawNulls(numPartitions_, nullptr);
  std::vector<uint64_t> bitOffsets(numPartitions_, 0);
  for (uint32_t p : nonEmptyPartitions) {
    const auto numWords = bits::nwords(context.rowCounts[p]) + 1;
    nulls[p] = AlignedBuffer::allocate<uint64_t>(numWords, pool_, 0);
    rawNulls[p] = nulls[p]->asMutable<uint64_t>();
  }

  for (size_t vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex) {
    const auto& pv = partitionedVectors[vectorIndex];
    const auto* partitionOffsets = pv->rawPartitionOffsets();
    const auto* parentNulls = rawNullsAt(context.parentNulls, vectorIndex);
    const auto encoding = pv->baseVector()->encoding();

    // validBits == nullptr means every present row in this batch is not null;
    // allNull means every present row is null. Otherwise validBits is a
    // full-row-space bitmap where a set bit means not null.
    const uint64_t* validBits{nullptr};
    bool allNull{false};
    BufferPtr dictionaryNulls;
    switch (encoding) {
      case VectorEncoding::Simple::FLAT:
        validBits = pv->baseVector()->rawNulls();
        break;
      case VectorEncoding::Simple::CONSTANT:
        allNull = pv->baseVector()->isNullAt(0);
        break;
      case VectorEncoding::Simple::DICTIONARY:
        dictionaryNulls = combinedNulls(*pv->baseVector(), pool_);
        validBits = dictionaryNulls ? dictionaryNulls->as<uint64_t>() : nullptr;
        break;
      case VectorEncoding::Simple::BIASED:
      case VectorEncoding::Simple::SEQUENCE:
        VELOX_NYI(
            "Unsupported vector encoding for PrestoIterativePartitioningSerializer: {}",
            encoding);
      default:
        VELOX_UNSUPPORTED(
            "Invalid vector encoding for PrestoIterativePartitioningSerializer: {}",
            encoding);
    }

    for (uint32_t p : nonEmptyPartitions) {
      const vector_size_t begin = p == 0 ? 0 : partitionOffsets[p - 1];
      const vector_size_t end = partitionOffsets[p];
      if (end == begin) {
        continue;
      }

      const vector_size_t present = parentNulls != nullptr
          ? bits::countBits(parentNulls, begin, end)
          : end - begin;
      if (allNull) {
        // Leave the compacted bits at 0 (null) and advance the cursor.
        nullCounts[p] += present;
      } else if (validBits == nullptr) {
        // No nulls in this batch: mark all present rows not null.
        bits::fillBits(
            rawNulls[p],
            bitOffsets[p],
            bitOffsets[p] + present,
            bits::kNotNull);
      } else {
        compactBits(
            validBits, parentNulls, begin, end, rawNulls[p], bitOffsets[p]);
        const auto valid = bits::countBits(
            rawNulls[p], bitOffsets[p], bitOffsets[p] + present);
        nullCounts[p] += present - valid;
      }
      bitOffsets[p] += present;
    }
  }

  flushNullSection(
      nonEmptyPartitions,
      outputStreams,
      context.rowCounts,
      nullCounts,
      rawNulls);
}

void PrestoIterativePartitioningSerializer::flushNullSection(
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const std::vector<vector_size_t>& rowCounts,
    const std::vector<vector_size_t>& nullCounts,
    const std::vector<uint64_t*>& validBits) const {
  for (uint32_t p : nonEmptyPartitions) {
    const char hasNulls = nullCounts[p] > 0 ? 1 : 0;
    outputStreams[p]->write(&hasNulls, 1);
  }

  for (uint32_t p : nonEmptyPartitions) {
    if (nullCounts[p] == 0) {
      continue;
    }
    // Convert Velox format (LSB-first, 1 == not null) to Presto wire format
    // (MSB-first, 1 == null). Pad bits past the row count stay not-null.
    const int32_t numRows = static_cast<int32_t>(rowCounts[p]);
    const int32_t numBytes = bits::nbytes(numRows);
    bits::fillBits(validBits[p], numRows, numBytes * 8, bits::kNotNull);
    auto* bytes = reinterpret_cast<uint8_t*>(validBits[p]);
    for (int32_t i = 0; i < numBytes; ++i) {
      bytes[i] = ~bytes[i];
      bits::reverseBits(&bytes[i], 1);
    }
    outputStreams[p]->write(reinterpret_cast<const char*>(bytes), numBytes);
  }
}

void PrestoIterativePartitioningSerializer::flushRowOffsets(
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams,
    const std::vector<vector_size_t>& rowCounts,
    const std::vector<vector_size_t>& nullCounts,
    const std::vector<uint64_t*>& validBits) const {
  for (uint32_t p : nonEmptyPartitions) {
    const int32_t numRows = static_cast<int32_t>(rowCounts[p]);

    if (nullCounts[p] == 0) {
      for (int32_t i = 0; i <= numRows; ++i) {
        writeInt32(outputStreams[p], i);
      }
      continue;
    }

    const uint64_t* valid = validBits[p];
    int32_t offset = 0;
    writeInt32(outputStreams[p], 0);
    for (int32_t i = 0; i < numRows; ++i) {
      offset += bits::isBitSet(valid, i);
      writeInt32(outputStreams[p], offset);
    }
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

void PrestoIterativePartitioningSerializer::flushOffsets(
    const ColumnBufferState& columnState,
    const std::vector<uint32_t>& nonEmptyPartitions,
    const std::vector<IOBufOutputStream*>& outputStreams) const {
  const auto* variableWidthState =
      dynamic_cast<const VariableWidthBufferState*>(&columnState);
  VELOX_DCHECK_NOT_NULL(variableWidthState);

  for (auto p : nonEmptyPartitions) {
    const auto& offsets = variableWidthState->offsetsAt(p);
    VELOX_DCHECK_EQ(offsets.size(), bufferState_->rowsPerPartition()[p]);

    if (!offsets.empty()) {
      outputStreams[p]->write(
          reinterpret_cast<const char*>(offsets.data()),
          offsets.size() * sizeof(int32_t));
    }
  }
}

} // namespace facebook::velox::serializer::presto
