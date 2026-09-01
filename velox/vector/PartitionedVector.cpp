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
#include "velox/vector/PartitionedVector.h"

#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

using Byte = uint8_t;
using BitIndex = uint8_t;

namespace {

inline void countPartitionSizes(
    const std::vector<uint32_t>& partitions,
    vector_size_t* rowCounts) {
  VELOX_DCHECK_NOT_NULL(rowCounts);

  // Restrict-qualify both pointers: `rowCounts` and the partition ids are
  // distinct arrays of the same element width, so without this hint the
  // compiler must assume the histogram store may clobber the next partition
  // id and reload it on every iteration.
  vector_size_t* __restrict counts = rowCounts;
  const uint32_t* __restrict parts = partitions.data();
  const auto numRows = static_cast<vector_size_t>(partitions.size());
  for (vector_size_t i = 0; i < numRows; ++i) {
    ++counts[parts[i]];
  }
}

inline void prefixSum(vector_size_t* offsets, uint32_t numPartitions) {
  for (uint32_t i = 1; i < numPartitions; i++) {
    offsets[i] += offsets[i - 1];
  }
}

inline void calculateOffsets(
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    vector_size_t vectorSize,
    uint32_t numPartitions,
    vector_size_t* endPartitionOffsets) {
  VELOX_DCHECK_NOT_NULL(endPartitionOffsets);

  std::fill_n(endPartitionOffsets, numPartitions, 0);

  if (singlePartition.has_value()) {
    VELOX_DCHECK_LT(singlePartition.value(), numPartitions);

    std::fill_n(
        endPartitionOffsets + singlePartition.value(),
        numPartitions - singlePartition.value(),
        vectorSize);
    return;
  }

  if (numPartitions > 1) {
    countPartitionSizes(partitions, endPartitionOffsets);
    prefixSum(endPartitionOffsets, numPartitions);
  } else {
    endPartitionOffsets[0] = static_cast<vector_size_t>(partitions.size());
  }
}

// endPartitionOffsets is an array of length numPartitions where each entry i is
// the exclusive end position of partition i. cursorPartitionOffsets is
// initialized such that cursorPartitionOffsets[0] = 0 and for i>0,
// cursorPartitionOffsets[i] = endPartitionOffsets[i-1], i.e., the inclusive
// begin positions.
void initializeCursorPartitionOffsets(
    BufferPtr& cursorPartitionOffsets,
    const BufferPtr& endPartitionOffsets,
    uint32_t numPartitions,
    velox::memory::MemoryPool* pool) {
  VELOX_DCHECK_NOT_NULL(endPartitionOffsets);
  VELOX_DCHECK_EQ(
      endPartitionOffsets->size(), numPartitions * sizeof(vector_size_t));

  ensureCapacity<vector_size_t>(cursorPartitionOffsets, numPartitions, pool);
  cursorPartitionOffsets->asMutable<vector_size_t>()[0] = 0;
  std::memcpy(
      &cursorPartitionOffsets->asMutable<vector_size_t>()[1],
      endPartitionOffsets->as<vector_size_t>(),
      sizeof(vector_size_t) * (numPartitions - 1));
  cursorPartitionOffsets->setSize(numPartitions * sizeof(vector_size_t));
}

// Scatter bits from `input` to `output` (which must be pre-zeroed) by reading
// each bit sequentially and writing it to the position given by
// cursorOffsets[partitions[i]]++. Sequential reads allow hardware prefetching;
// with few partitions the write streams stay cache-resident.
void scatterBits(
    const uint8_t* input,
    uint8_t* output,
    vector_size_t numRows,
    const std::vector<uint32_t>& partitions,
    vector_size_t* cursorOffsets) {
  for (vector_size_t i = 0; i < numRows; ++i) {
    const auto destPos = cursorOffsets[partitions[i]]++;
    const uint8_t bit = (input[i >> 3] >> (i & 7)) & 1;
    output[destPos >> 3] |= static_cast<uint8_t>(bit << (destPos & 7));
  }
}

// Scatter-in-place: scatter bits from `inout` into `temp`, then copy back.
void scatterBitsInPlace(
    uint8_t* inout,
    uint8_t* temp,
    vector_size_t numRows,
    const std::vector<uint32_t>& partitions,
    vector_size_t* cursorOffsets) {
  const auto numBytes = bits::nbytes(numRows);
  std::memset(temp, 0, numBytes);
  scatterBits(inout, temp, numRows, partitions, cursorOffsets);
  std::memcpy(inout, temp, numBytes);
}

// Scatters 'numRows' values from 'input' to 'output'. Row i is written to
// output[cursor[partitions[i]]], after which cursor[partitions[i]] is advanced.
// cursor[p] holds the next free output slot of partition p and is updated in
// place. The four pointers reference non-overlapping memory; the '__restrict'
// qualifiers tell the compiler so, which lets it pipeline the per-row loads
// instead of reloading the partition id and cursor after every scatter store,
// which it would otherwise have to assume the store may alias.
template <typename T>
void scatterPartition(
    const T* __restrict input,
    T* __restrict output,
    const uint32_t* __restrict partitions,
    vector_size_t numRows,
    vector_size_t* __restrict cursor) {
  vector_size_t i = 0;
  // Unroll by four so the independent address computations of distinct
  // partitions issue back to back. Rows that share a partition still serialize
  // through the cursor[p] read-modify-write.
  for (; i + 4 <= numRows; i += 4) {
    output[cursor[partitions[i]]++] = input[i];
    output[cursor[partitions[i + 1]]++] = input[i + 1];
    output[cursor[partitions[i + 2]]++] = input[i + 2];
    output[cursor[partitions[i + 3]]++] = input[i + 3];
  }
  for (; i < numRows; ++i) {
    output[cursor[partitions[i]]++] = input[i];
  }
}

template <typename T>
void partitionFixedWidthValues(
    BufferPtr& inputBuffer,
    const std::vector<uint32_t>& partitions,
    const BufferPtr& endPartitionOffsets,
    uint32_t numPartitions,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  VELOX_DCHECK_NOT_NULL(inputBuffer);

  const auto numRows = static_cast<vector_size_t>(partitions.size());
  initializeCursorPartitionOffsets(
      ctx.cursorPartitionOffsets, endPartitionOffsets, numPartitions, pool);
  auto* rawCursorOffsets =
      ctx.cursorPartitionOffsets->asMutable<vector_size_t>();

  // Allocate ctx.tempBuffer from the input buffer's pool, not 'pool'. The
  // swap below installs ctx.tempBuffer into the caller's FlatVector; using
  // the input's pool keeps buffer ownership inside the caller's pool and
  // avoids the cross-pool leak that the operator's pool would otherwise
  // report when the caller's vector outlives it.
  auto* inputPool = inputBuffer->pool();
  if (ctx.tempBuffer != nullptr && ctx.tempBuffer->pool() != inputPool) {
    ctx.tempBuffer.reset();
  }
  ensureCapacity<T>(ctx.tempBuffer, numRows, inputPool);
  auto* input = inputBuffer->asMutable<T>();
  auto* output = ctx.tempBuffer->asMutable<T>();
  scatterPartition<T>(
      input, output, partitions.data(), numRows, rawCursorOffsets);
  // ensureCapacity leaves ctx.tempBuffer->size_ at its previous value when
  // capacity already suffices, so set it to the number of values just
  // written before swapping. Without this, FlatVector::slice() reads
  // size/sizeof(T) and can underflow to 0 for a narrower previous column
  // (e.g. int32 followed by int64).
  ctx.tempBuffer->setSize(BaseVector::byteSize<T>(numRows));
  // Swap: inputBuffer (now in the caller's FlatVector after the swap) holds
  // the partitioned output; ctx.tempBuffer retains the old input buffer for
  // reuse on the next call.
  std::swap(inputBuffer, ctx.tempBuffer);
}

template <>
void partitionFixedWidthValues<bool>(
    BufferPtr& inputBuffer,
    const std::vector<uint32_t>& partitions,
    const BufferPtr& endPartitionOffsets,
    uint32_t numPartitions,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  VELOX_DCHECK_NOT_NULL(inputBuffer);

  const auto numRows = static_cast<vector_size_t>(partitions.size());
  const auto numBytes = bits::nbytes(numRows);
  initializeCursorPartitionOffsets(
      ctx.cursorPartitionOffsets, endPartitionOffsets, numPartitions, pool);
  auto* rawCursorOffsets =
      ctx.cursorPartitionOffsets->asMutable<vector_size_t>();

  ensureCapacity<uint8_t>(ctx.tempBuffer, numBytes, pool);
  scatterBitsInPlace(
      inputBuffer->asMutable<uint8_t>(),
      ctx.tempBuffer->asMutable<uint8_t>(),
      numRows,
      partitions,
      rawCursorOffsets);
}

template <TypeKind kind>
void partitionDictionaryIndices(
    const VectorPtr& vector,
    const std::vector<uint32_t>& partitions,
    const BufferPtr& endPartitionOffsets,
    uint32_t numPartitions,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto* dictionaryVector = vector->as<DictionaryVector<T>>();
  VELOX_DCHECK_NOT_NULL(dictionaryVector);

  const auto inputIndices = dictionaryVector->indices();
  auto& cachedIndices = ctx.partitionedDictionaryIndices;
  if (const auto it = cachedIndices.find(inputIndices);
      it != cachedIndices.end()) {
    dictionaryVector->setIndices(it->second);
    return;
  }

  initializeCursorPartitionOffsets(
      ctx.cursorPartitionOffsets, endPartitionOffsets, numPartitions, pool);
  auto* rawCursorOffsets =
      ctx.cursorPartitionOffsets->asMutable<vector_size_t>();

  auto partitionedIndices =
      AlignedBuffer::allocate<vector_size_t>(vector->size(), pool);
  scatterPartition<vector_size_t>(
      inputIndices->template as<vector_size_t>(),
      partitionedIndices->asMutable<vector_size_t>(),
      partitions.data(),
      vector->size(),
      rawCursorOffsets);
  cachedIndices.emplace(inputIndices, partitionedIndices);
  dictionaryVector->setIndices(partitionedIndices);
}

template <TypeKind typeKind>
PartitionedVectorPtr createPartitionedFlatVector(
    VectorPtr vector,
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    uint32_t numPartitions,
    const BufferPtr& endPartitionOffsets,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  using T = typename TypeTraits<typeKind>::NativeType;
  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(vector);
  VELOX_CHECK_NOT_NULL(flatVector);

  auto partitionedFlatVector = std::make_shared<PartitionedFlatVector<T>>(
      flatVector, numPartitions, endPartitionOffsets, pool);

  // Always call partition() so that numNullsPerPartition_ is populated,
  // even when numPartitions == 1 and no data movement is required.
  partitionedFlatVector->partition(partitions, singlePartition, ctx);

  return partitionedFlatVector;
}

PartitionedVectorPtr createPartitionedRowVector(
    VectorPtr vector,
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    uint32_t numPartitions,
    const BufferPtr& endPartitionOffsets,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
  VELOX_CHECK_NOT_NULL(rowVector);

  auto partitionedRowVector = std::make_shared<PartitionedRowVector>(
      rowVector, numPartitions, endPartitionOffsets, pool);

  // Always call partition() to initialize partitionedChildren_, even when
  // numPartitions == 1, so that partitionAt() can reconstruct the RowVector.
  partitionedRowVector->partition(partitions, singlePartition, ctx);

  return partitionedRowVector;
}

PartitionedVectorPtr createPartitionedDictionaryVector(
    VectorPtr vector,
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    uint32_t numPartitions,
    const BufferPtr& endPartitionOffsets,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  VELOX_DCHECK_EQ(vector->encoding(), VectorEncoding::Simple::DICTIONARY);

  auto partitionedDictionaryVector =
      std::make_shared<PartitionedDictionaryVector>(
          vector, numPartitions, endPartitionOffsets, pool);
  partitionedDictionaryVector->partition(partitions, singlePartition, ctx);
  return partitionedDictionaryVector;
}

} // namespace

PartitionedVector::~PartitionedVector() = default;

// public
PartitionedVectorPtr PartitionedVector::create(
    const VectorPtr& vector,
    const std::vector<uint32_t>& partitions,
    uint32_t numPartitions,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  return create(vector, partitions, std::nullopt, numPartitions, ctx, pool);
}

// public
PartitionedVectorPtr PartitionedVector::create(
    const VectorPtr& vector,
    uint32_t singlePartition,
    uint32_t numPartitions,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  return create(vector, {}, singlePartition, numPartitions, ctx, pool);
}

// protected
PartitionedVectorPtr PartitionedVector::create(
    const VectorPtr& vector,
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    uint32_t numPartitions,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(vector);
  VELOX_CHECK_GT(numPartitions, 0);
  if (singlePartition.has_value()) {
    VELOX_CHECK_LT(singlePartition.value(), numPartitions);
  } else {
    VELOX_CHECK_EQ(vector->size(), partitions.size());
  }
  VELOX_CHECK_NOT_NULL(pool);

  ctx.partitionedDictionaryIndices.clear();
  ctx.partitionedVectors.clear();

  // Calculate the end offsets for each partition. For example, if there are 3
  // partitions with 2, 3, and 1 rows respectively, then endPartitionOffsets[0]
  // = 2, endPartitionOffsets[1] = 5, and endPartitionOffsets[2] = 6.
  BufferPtr endPartitionOffsets;
  ensureCapacity<vector_size_t>(endPartitionOffsets, numPartitions, pool);
  calculateOffsets(
      partitions,
      singlePartition,
      vector->size(),
      numPartitions,
      endPartitionOffsets->asMutable<vector_size_t>());
  endPartitionOffsets->setSize(numPartitions * sizeof(vector_size_t));

  auto raw = endPartitionOffsets->as<vector_size_t>();
  VELOX_DCHECK_EQ(raw[numPartitions - 1], vector->size());

  return create(
      vector,
      partitions,
      singlePartition,
      numPartitions,
      endPartitionOffsets,
      ctx,
      pool);
}

PartitionedVectorPtr PartitionedVector::create(
    const VectorPtr& vector,
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    uint32_t numPartitions,
    const BufferPtr& endPartitionOffsets,
    PartitionBuildContext& ctx,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(endPartitionOffsets);
  VELOX_CHECK_EQ(
      endPartitionOffsets->size(), numPartitions * sizeof(vector_size_t));

  // Partition shared vectors only once because partitioning mutates buffers.
  if (const auto it = ctx.partitionedVectors.find(vector.get());
      it != ctx.partitionedVectors.end()) {
    return it->second;
  }

  auto encoding = vector->encoding();
  auto typeKind = vector->typeKind();

  PartitionedVectorPtr partitionedVector;
  switch (encoding) {
    case VectorEncoding::Simple::FLAT: {
      partitionedVector = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          createPartitionedFlatVector,
          typeKind,
          vector,
          partitions,
          singlePartition,
          numPartitions,
          endPartitionOffsets,
          ctx,
          pool);
      break;
    }

    case VectorEncoding::Simple::ROW: {
      partitionedVector = createPartitionedRowVector(
          vector,
          partitions,
          singlePartition,
          numPartitions,
          endPartitionOffsets,
          ctx,
          pool);
      break;
    }

    case VectorEncoding::Simple::CONSTANT: {
      auto partitionedConstantVector =
          std::make_shared<PartitionedConstantVector>(
              vector, numPartitions, endPartitionOffsets, pool);
      partitionedConstantVector->partition(partitions, singlePartition, ctx);
      partitionedVector = std::move(partitionedConstantVector);
      break;
    }

    case VectorEncoding::Simple::DICTIONARY: {
      // Push dictionary encoding to the leaf vectors for RowVectors,
      // or combine adjacent DICT layers for non RowVectors.
      const auto rewrittenVector =
          RowVector::pushDictionaryToRowVectorLeaves(vector);

      if (rewrittenVector->encoding() == VectorEncoding::Simple::DICTIONARY) {
        partitionedVector = createPartitionedDictionaryVector(
            rewrittenVector,
            partitions,
            singlePartition,
            numPartitions,
            endPartitionOffsets,
            ctx,
            pool);
      } else {
        partitionedVector = create(
            rewrittenVector,
            partitions,
            singlePartition,
            numPartitions,
            endPartitionOffsets,
            ctx,
            pool);
      }
      break;
    }

    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::SEQUENCE:
    case VectorEncoding::Simple::LAZY:
      VELOX_UNSUPPORTED(
          "Unsupported vector encoding for PartitionedVector: {}",
          mapSimpleToName(encoding));
    default:
      VELOX_UNREACHABLE(
          "Invalid vector encoding for PartitionedVector: {}", encoding);
  }

  ctx.partitionedVectors.emplace(vector.get(), partitionedVector);
  return partitionedVector;
}

VectorPtr PartitionedVector::baseVector() const {
  return vector_;
}

std::string PartitionedVector::toString() const {
  std::string offsets;
  for (vector_size_t i = 0; i < numPartitions_; ++i) {
    if (i > 0) {
      offsets += ',';
    }
    offsets += fmt::format("{}", rawEndPartitionOffsets_[i]);
  }

  return fmt::format(
      "PartitionedVector[numPartitions: {}, offsets: {}]",
      numPartitions_,
      offsets);
}

template <typename T>
void PartitionedFlatVector<T>::partition(
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    PartitionBuildContext& ctx) {
  if (singlePartition.has_value()) {
    if (const auto* rawNulls = vector_->rawNulls()) {
      numNullsPerPartition_[singlePartition.value()] =
          static_cast<vector_size_t>(
              bits::countNulls(rawNulls, 0, vector_->size()));
    }
    return;
  }

  if (vector_->rawNulls()) {
    const auto numRows = static_cast<vector_size_t>(partitions.size());
    const auto numBytes = bits::nbytes(numRows);
    initializeCursorPartitionOffsets(
        ctx.cursorPartitionOffsets,
        endPartitionOffsets_,
        numPartitions_,
        pool_);
    ensureCapacity<uint8_t>(ctx.tempBuffer, numBytes, pool_);
    scatterBitsInPlace(
        reinterpret_cast<uint8_t*>(vector_->mutableRawNulls()),
        ctx.tempBuffer->asMutable<uint8_t>(),
        numRows,
        partitions,
        ctx.cursorPartitionOffsets->asMutable<vector_size_t>());
  }

  auto* flatVector = vector_->as<FlatVector<T>>();
  // Take a local ref-counted copy so partitionFixedWidthValues can swap it.
  auto valuesBuffer = flatVector->values();
  partitionFixedWidthValues<T>(
      valuesBuffer,
      partitions,
      endPartitionOffsets_,
      numPartitions_,
      ctx,
      pool_);
  // Install the (swapped-in) partitioned buffer; ctx.tempBuffer now holds
  // the old one.
  flatVector->unsafeSetValues(std::move(valuesBuffer));

  // Count nulls per partition from the now-partitioned null bitmap.
  if (const uint64_t* rawNulls = vector_->rawNulls()) {
    for (uint32_t p = 0; p < numPartitions_; ++p) {
      const vector_size_t begin = p == 0 ? 0 : rawEndPartitionOffsets_[p - 1];
      const vector_size_t end = rawEndPartitionOffsets_[p];
      if (begin < end) {
        numNullsPerPartition_[p] =
            static_cast<vector_size_t>(bits::countNulls(rawNulls, begin, end));
      }
    }
  }
}

template <typename T>
VectorPtr PartitionedFlatVector<T>::partitionAt(uint32_t partition) const {
  VELOX_CHECK_LT(partition, numPartitions_);

  vector_size_t beginOffset =
      partition == 0 ? 0 : rawEndPartitionOffsets_[partition - 1];
  vector_size_t numRowsInPartition =
      rawEndPartitionOffsets_[partition] - beginOffset;

  return vector_->slice(beginOffset, numRowsInPartition);
}

void PartitionedRowVector::partition(
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    PartitionBuildContext& ctx) {
  auto* rowVector = vector_->as<RowVector>();
  partitionedChildren_.reserve(rowVector->childrenSize());

  for (const auto& child : rowVector->children()) {
    partitionedChildren_.push_back(
        PartitionedVector::create(
            child,
            partitions,
            singlePartition,
            numPartitions_,
            endPartitionOffsets_,
            ctx,
            pool_));
  }

  if (singlePartition.has_value()) {
    if (const auto* rawNulls = vector_->rawNulls()) {
      numNullsPerPartition_[singlePartition.value()] =
          static_cast<vector_size_t>(
              bits::countNulls(rawNulls, 0, vector_->size()));
    }
    return;
  }

  if (numPartitions_ > 1 && vector_->rawNulls()) {
    const auto numRows = static_cast<vector_size_t>(partitions.size());
    const auto numBytes = bits::nbytes(numRows);
    initializeCursorPartitionOffsets(
        ctx.cursorPartitionOffsets,
        endPartitionOffsets_,
        numPartitions_,
        pool_);
    ensureCapacity<uint8_t>(ctx.tempBuffer, numBytes, pool_);
    scatterBitsInPlace(
        reinterpret_cast<uint8_t*>(vector_->mutableRawNulls()),
        ctx.tempBuffer->asMutable<uint8_t>(),
        numRows,
        partitions,
        ctx.cursorPartitionOffsets->asMutable<vector_size_t>());
  }

  // Count nulls per partition from the now-partitioned null bitmap.
  if (const uint64_t* rawNulls = vector_->rawNulls()) {
    for (uint32_t p = 0; p < numPartitions_; ++p) {
      const vector_size_t begin = p == 0 ? 0 : rawEndPartitionOffsets_[p - 1];
      const vector_size_t end = rawEndPartitionOffsets_[p];
      if (begin < end) {
        numNullsPerPartition_[p] =
            static_cast<vector_size_t>(bits::countNulls(rawNulls, begin, end));
      }
    }
  }
}

VectorPtr PartitionedRowVector::partitionAt(uint32_t partition) const {
  VELOX_CHECK_LT(partition, numPartitions_);

  vector_size_t beginOffset =
      partition == 0 ? 0 : rawEndPartitionOffsets_[partition - 1];
  vector_size_t numRowsInPartition =
      rawEndPartitionOffsets_[partition] - beginOffset;

  std::vector<VectorPtr> children;
  children.reserve(partitionedChildren_.size());
  for (const auto& child : partitionedChildren_) {
    children.push_back(child->partitionAt(partition));
  }

  BufferPtr nulls = nullptr;
  if (numRowsInPartition > 0 && vector_->rawNulls()) {
    nulls = AlignedBuffer::allocate<bool>(numRowsInPartition, pool_);
    bits::copyBits(
        vector_->rawNulls(),
        beginOffset,
        nulls->asMutable<uint64_t>(),
        0,
        numRowsInPartition);
  }

  return std::make_shared<RowVector>(
      pool_,
      vector_->type(),
      std::move(nulls),
      numRowsInPartition,
      std::move(children));
}

void PartitionedConstantVector::partition(
    const std::vector<uint32_t>& /*partitions*/,
    std::optional<uint32_t> singlePartition,
    PartitionBuildContext& /*ctx*/) {
  if (!vector_->isNullAt(0)) {
    return;
  }

  if (singlePartition.has_value()) {
    numNullsPerPartition_[singlePartition.value()] = vector_->size();
    return;
  }

  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const vector_size_t begin = p == 0 ? 0 : rawEndPartitionOffsets_[p - 1];
    const vector_size_t end = rawEndPartitionOffsets_[p];
    if (begin < end) {
      numNullsPerPartition_[p] = end - begin;
    }
  }
}

VectorPtr PartitionedConstantVector::partitionAt(uint32_t partition) const {
  VELOX_CHECK_LT(partition, numPartitions_);

  const vector_size_t beginOffset =
      partition == 0 ? 0 : rawEndPartitionOffsets_[partition - 1];
  const vector_size_t numRowsInPartition =
      rawEndPartitionOffsets_[partition] - beginOffset;

  return vector_->slice(0, numRowsInPartition);
}

void PartitionedDictionaryVector::partition(
    const std::vector<uint32_t>& partitions,
    std::optional<uint32_t> singlePartition,
    PartitionBuildContext& ctx) {
  if (singlePartition.has_value()) {
    if (vector_->mayHaveNulls()) {
      vector_size_t numNulls = 0;
      for (vector_size_t row = 0; row < vector_->size(); ++row) {
        numNulls += vector_->isNullAt(row);
      }
      numNullsPerPartition_[singlePartition.value()] = numNulls;
    }
    return;
  }

  if (numPartitions_ > 1) {
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        partitionDictionaryIndices,
        vector_->typeKind(),
        vector_,
        partitions,
        endPartitionOffsets_,
        numPartitions_,
        ctx,
        pool_);

    if (vector_->rawNulls()) {
      const auto numRows = static_cast<vector_size_t>(partitions.size());
      const auto numBytes = bits::nbytes(numRows);
      initializeCursorPartitionOffsets(
          ctx.cursorPartitionOffsets,
          endPartitionOffsets_,
          numPartitions_,
          pool_);
      ensureCapacity<uint8_t>(ctx.tempBuffer, numBytes, pool_);
      scatterBitsInPlace(
          reinterpret_cast<uint8_t*>(vector_->mutableRawNulls()),
          ctx.tempBuffer->asMutable<uint8_t>(),
          numRows,
          partitions,
          ctx.cursorPartitionOffsets->asMutable<vector_size_t>());
    }
  }

  if (!vector_->mayHaveNulls()) {
    return;
  }

  for (uint32_t p = 0; p < numPartitions_; ++p) {
    const vector_size_t begin = p == 0 ? 0 : rawEndPartitionOffsets_[p - 1];
    const vector_size_t end = rawEndPartitionOffsets_[p];
    if (begin < end) {
      for (vector_size_t row = begin; row < end; ++row) {
        numNullsPerPartition_[p] += vector_->isNullAt(row);
      }
    }
  }
}

VectorPtr PartitionedDictionaryVector::partitionAt(uint32_t partition) const {
  VELOX_CHECK_LT(partition, numPartitions_);

  const vector_size_t beginOffset =
      partition == 0 ? 0 : rawEndPartitionOffsets_[partition - 1];
  const vector_size_t numRowsInPartition =
      rawEndPartitionOffsets_[partition] - beginOffset;

  return vector_->slice(beginOffset, numRowsInPartition);
}

} // namespace facebook::velox
