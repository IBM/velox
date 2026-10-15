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
#include "velox/exec/OptimizedHashPartitionFunction.h"

#include <algorithm>

#include <folly/Portability.h>

#include "velox/common/process/ProcessBase.h"

#if defined(__AVX2__) || defined(__AVX512F__)
#include <immintrin.h>
#endif

#define XXH_INLINE_ALL
#include <xxhash.h> // @manual=third-party//xxHash:xxhash

namespace facebook::velox::exec {
namespace {
// Gets the hash value for local exchange with given 'rawHash'. 'rawHash'
// is the value computed by this hash function which is used for remote
// shuffle across stages like for Prestissimo.
static inline uint32_t localExchangeHash(uint32_t rawHash) {
  // Mix the bits so we don't use the same hash used to distribute between
  // stages.
  bits::reverseBits(reinterpret_cast<uint8_t*>(&rawHash), sizeof(rawHash));
  return XXH32(&rawHash, sizeof(rawHash), 0);
}

FOLLY_ALWAYS_INLINE uint32_t mixedHash(uint64_t hash) {
  return static_cast<uint32_t>(hash) ^ static_cast<uint32_t>(hash >> 32);
}

FOLLY_ALWAYS_INLINE uint32_t
reduceRange(uint64_t hash, uint32_t numPartitions) {
  return (static_cast<uint64_t>(mixedHash(hash)) * numPartitions) >> 32;
}

void rangeReductionPowerOfTwo(
    const uint64_t* hashes,
    uint32_t* partitions,
    vector_size_t size,
    uint32_t numPartitions) {
  VELOX_DCHECK(bits::isPowerOfTwo(numPartitions));

  if (numPartitions == 1) {
    std::fill(partitions, partitions + size, 0);
    return;
  }

  const auto shift = 32 - __builtin_ctz(numPartitions);
  for (vector_size_t index = 0; index < size; ++index) {
    partitions[index] = mixedHash(hashes[index]) >> shift;
  }
}

#if defined(__AVX512F__)
void rangeReductionAvx512(
    const uint64_t* hashes,
    uint32_t* partitions,
    vector_size_t size,
    uint32_t numPartitions) {
  const __m512i numPartitionsVec = _mm512_set1_epi64(numPartitions);

  vector_size_t index = 0;
  for (; index + 8 <= size; index += 8) {
    const auto hashesVec =
        _mm512_loadu_si512(reinterpret_cast<const __m512i*>(hashes + index));

    const auto mixedHashesVec =
        _mm512_xor_si512(hashesVec, _mm512_srli_epi64(hashesVec, 32));
    const auto productVec = _mm512_mul_epu32(mixedHashesVec, numPartitionsVec);
    const auto shiftedVec = _mm512_srli_epi64(productVec, 32);
    const auto packedResults = _mm512_cvtepi64_epi32(shiftedVec);
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(partitions + index), packedResults);
  }

  for (; index < size; ++index) {
    partitions[index] = reduceRange(hashes[index], numPartitions);
  }
}
#endif

#if defined(__AVX2__)
void rangeReductionAvx2(
    const uint64_t* hashes,
    uint32_t* partitions,
    vector_size_t size,
    uint32_t numPartitions) {
  const auto packIndexes = _mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0);
  const auto numPartitionsVec = _mm256_set1_epi64x(numPartitions);

  vector_size_t index = 0;
  for (; index + 4 <= size; index += 4) {
    const auto hashesVec =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hashes + index));
    const auto mixedHashesVec =
        _mm256_xor_si256(hashesVec, _mm256_srli_epi64(hashesVec, 32));
    const auto productVec = _mm256_mul_epu32(mixedHashesVec, numPartitionsVec);
    const auto shiftedVec = _mm256_srli_epi64(productVec, 32);
    const auto packedResults =
        _mm256_permutevar8x32_epi32(shiftedVec, packIndexes);
    _mm_storeu_si128(
        reinterpret_cast<__m128i*>(partitions + index),
        _mm256_castsi256_si128(packedResults));
  }

  for (; index < size; ++index) {
    partitions[index] = reduceRange(hashes[index], numPartitions);
  }
}
#endif

void rangeReductionImpl(
    const uint64_t* hashes,
    uint32_t* partitions,
    vector_size_t size,
    uint32_t numPartitions) {
  if (bits::isPowerOfTwo(numPartitions)) {
    rangeReductionPowerOfTwo(hashes, partitions, size, numPartitions);
    return;
  }

#if defined(__AVX512F__)
  if (process::hasAvx512f()) {
    rangeReductionAvx512(hashes, partitions, size, numPartitions);
    return;
  }
#endif

#if defined(__AVX2__)
  if (process::hasAvx2()) {
    rangeReductionAvx2(hashes, partitions, size, numPartitions);
    return;
  }
#endif

  for (vector_size_t index = 0; index < size; ++index) {
    partitions[index] = reduceRange(hashes[index], numPartitions);
  }
}

void applyLocalExchangeHash(raw_vector<uint64_t>& hashes) {
  for (auto& hash : hashes) {
    hash = localExchangeHash(hash);
  }
}

void applyHashBitRange(
    const HashBitRange& hashBitRange,
    const raw_vector<uint64_t>& hashes,
    std::vector<uint32_t>& partitions) {
  partitions.resize(hashes.size());
  for (auto index = 0; index < hashes.size(); ++index) {
    partitions[index] = hashBitRange.partition(hashes[index]);
  }
}

bool allConstantKeys(
    const RowVector& input,
    const std::vector<std::unique_ptr<OptimizedVectorHasher>>& hashers) {
  for (const auto& hasher : hashers) {
    if (hasher->channel() != kConstantChannel &&
        !input.childAt(hasher->channel())->isConstantEncoding()) {
      return false;
    }
  }
  return true;
}

} // namespace

void rangeReduction(
    const uint64_t* hashes,
    uint32_t* partitions,
    vector_size_t size,
    uint32_t numPartitions) {
  rangeReductionImpl(hashes, partitions, size, numPartitions);
}

OptimizedHashPartitionFunction::OptimizedHashPartitionFunction(
    bool localExchange,
    int numPartitions,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues)
    : localExchange_{localExchange}, numPartitions_{numPartitions} {
  init(inputType, keyChannels, constValues);
}

OptimizedHashPartitionFunction::OptimizedHashPartitionFunction(
    const HashBitRange& hashBitRange,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues)
    : localExchange_{false},
      numPartitions_{hashBitRange.numPartitions()},
      hashBitRange_(hashBitRange) {
  VELOX_CHECK_GT(hashBitRange.numPartitions(), 0);
  VELOX_CHECK(!keyChannels.empty());
  init(inputType, keyChannels, constValues);
}

std::optional<uint32_t> OptimizedHashPartitionFunction::partition(
    const RowVector& input,
    std::vector<uint32_t>& partitions) {
  if (hashers_.empty()) {
    return 0u;
  }

  const auto size = input.size();
  if (size == 0) {
    partitions.clear();
    return std::nullopt;
  }

  if (!hashBitRange_.has_value() && numPartitions_ == 1) {
    return 0u;
  }

  rows_.resize(size);
  rows_.setAll();

  if (allConstantKeys(input, hashers_)) {
    uint64_t hash{0};
    for (auto i = 0; i < hashers_.size(); ++i) {
      auto& hasher = hashers_[i];
      if (hasher->channel() != kConstantChannel) {
        hasher->decode(*input.childAt(hasher->channel()), rows_);
        const auto hashValue = hasher->hashConstant(i > 0, hash);
        VELOX_DCHECK(hashValue.has_value());
        hash = hashValue.value();
      } else {
        hash = hasher->hashPrecomputed(i > 0, hash);
      }
    }

    if (localExchange_) {
      hash = localExchangeHash(hash);
    }

    return hashBitRange_.has_value() ? hashBitRange_->partition(hash)
                                     : reduceRange(hash, numPartitions_);
  }

  hashes_.resize(size);
  for (auto i = 0; i < hashers_.size(); ++i) {
    auto& hasher = hashers_[i];
    if (hasher->channel() != kConstantChannel) {
      hashers_[i]->decode(*input.childAt(hasher->channel()), rows_);
      hashers_[i]->hash(rows_, i > 0, hashes_);
    } else {
      hashers_[i]->hashPrecomputed(i > 0, hashes_);
    }
  }

  if (localExchange_) {
    applyLocalExchangeHash(hashes_);
  }

  if (hashBitRange_.has_value()) {
    applyHashBitRange(*hashBitRange_, hashes_, partitions);
  } else {
    partitions.resize(size);
    rangeReduction(hashes_.data(), partitions.data(), size, numPartitions_);
  }

  return std::nullopt;
}

void OptimizedHashPartitionFunction::init(
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues) {
  hashers_.reserve(keyChannels.size());
  size_t constChannel{0};
  for (const auto channel : keyChannels) {
    if (channel != kConstantChannel) {
      hashers_.emplace_back(
          OptimizedVectorHasher::create(inputType->childAt(channel), channel));
    } else {
      const auto& constValue = constValues[constChannel++];
      hashers_.emplace_back(
          OptimizedVectorHasher::create(constValue->type(), channel));
      hashers_.back()->precompute(*constValue);
    }
  }
}

} // namespace facebook::velox::exec
