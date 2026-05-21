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

#include "velox/exec/OptimizedVectorHasher.h"

#include "velox/common/base/SimdUtil.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox::exec {
namespace {

template <bool typeProvidesCustomComparison, TypeKind Kind>
uint64_t hashOne(const DecodedVector& decoded, vector_size_t index) {
  if constexpr (
      Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
      Kind == TypeKind::MAP) {
    return decoded.base()->hashValueAt(decoded.index(index));
  } else {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    const T value = decoded.valueAt<T>(index);

    if constexpr (typeProvidesCustomComparison) {
      return static_cast<const CanProvideCustomComparisonType<Kind>*>(
                 decoded.base()->type().get())
          ->hash(value);
    } else if constexpr (std::is_floating_point_v<T>) {
      return util::floating_point::NaNAwareHash<T>()(value);
    } else {
      return folly::hasher<T>()(value);
    }
  }
}

constexpr uint64_t kNullHash = OptimizedVectorHasher::kNullHash;

// Fills `result[0..size)` with `hash`, mixing into the existing values when
// `Mix` is true.
template <bool Mix>
inline void broadcastHash(vector_size_t size, uint64_t* result, uint64_t hash) {
  if constexpr (Mix) {
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = bits::hashMix(result[i], hash);
    }
  } else {
    std::fill(result, result + size, hash);
  }
}

// Computes one hash per row via `computeHash(i)`. Caller guarantees no nulls.
template <bool Mix, typename ComputeHash>
inline void
hashLoopNoNulls(vector_size_t size, uint64_t* result, ComputeHash computeHash) {
  if constexpr (Mix) {
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = bits::hashMix(result[i], computeHash(i));
    }
  } else {
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = computeHash(i);
    }
  }
}

// Computes one hash per row, substituting `kNullHash` for null rows.
template <bool Mix, typename ComputeHash>
inline void hashLoopWithNulls(
    vector_size_t size,
    uint64_t* result,
    const DecodedVector& decoded,
    ComputeHash computeHash) {
  if constexpr (Mix) {
    for (vector_size_t i = 0; i < size; ++i) {
      const uint64_t hash = decoded.isNullAt(i) ? kNullHash : computeHash(i);
      result[i] = bits::hashMix(result[i], hash);
    }
  } else {
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = decoded.isNullAt(i) ? kNullHash : computeHash(i);
    }
  }
}

template <bool Mix>
inline void scatterDictionaryHashes(
    vector_size_t size,
    uint64_t* result,
    const vector_size_t* indices,
    const uint64_t* baseHashes) {
  if constexpr (Mix) {
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = bits::hashMix(result[i], baseHashes[indices[i]]);
    }
  } else {
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] = baseHashes[indices[i]];
    }
  }
}

template <bool Mix>
inline void scatterDictionaryHashesWithExtraNulls(
    vector_size_t size,
    uint64_t* result,
    const vector_size_t* indices,
    const uint64_t* nulls,
    const uint64_t* baseHashes) {
  if constexpr (Mix) {
    for (vector_size_t i = 0; i < size; ++i) {
      const uint64_t hash =
          bits::isBitNull(nulls, i) ? kNullHash : baseHashes[indices[i]];
      result[i] = bits::hashMix(result[i], hash);
    }
  } else {
    for (vector_size_t i = 0; i < size; ++i) {
      result[i] =
          bits::isBitNull(nulls, i) ? kNullHash : baseHashes[indices[i]];
    }
  }
}

/// converts Velox’s packed boolean storage into one hash per row.
/// @param values: a bitmap: one bit per row, where set means true and unset
/// means false
template <bool Mix>
inline void scatterBoolHashes(
    vector_size_t size,
    uint64_t* result,
    const uint64_t* values,
    const uint64_t* nulls) {
  using Batch = xsimd::batch<int64_t>;
  static constexpr vector_size_t kSimdBatchSize = Batch::size;
  const auto falseHash = folly::hasher<bool>()(false);
  const auto trueHash = folly::hasher<bool>()(true);

  vector_size_t row{0};
  if constexpr (!Mix) {
    const auto falseHashBatch =
        xsimd::broadcast<int64_t>(static_cast<int64_t>(falseHash));
    const auto trueHashBatch =
        xsimd::broadcast<int64_t>(static_cast<int64_t>(trueHash));
    const auto nullHashBatch =
        xsimd::broadcast<int64_t>(static_cast<int64_t>(kNullHash));
    auto* const signedResult = reinterpret_cast<int64_t*>(result);

    for (; row + kSimdBatchSize <= size; row += kSimdBatchSize) {
      const auto bitOffset = row & 63;
      const auto valueBits = (values[row / 64] >> bitOffset) &
          bits::lowMask(static_cast<int32_t>(kSimdBatchSize));
      auto hashes = xsimd::select(
          simd::fromBitMask<int64_t>(valueBits), trueHashBatch, falseHashBatch);

      if (nulls != nullptr) {
        const auto notNullBits = (nulls[row / 64] >> bitOffset) &
            bits::lowMask(static_cast<int32_t>(kSimdBatchSize));
        hashes = xsimd::select(
            simd::fromBitMask<int64_t>(notNullBits), hashes, nullHashBatch);
      }

      hashes.store_unaligned(signedResult + row);
    }
  }

  // TODO: improve performance
  for (; row < size; ++row) {
    const auto hash = nulls != nullptr && bits::isBitNull(nulls, row)
        ? kNullHash
        : (bits::isBitSet(values, row) ? trueHash : falseHash);
    if constexpr (Mix) {
      result[row] = bits::hashMix(result[row], hash);
    } else {
      result[row] = hash;
    }
  }
}

// Dispatches `body` with `Mix` resolved as a compile-time bool.
template <typename Body>
inline void dispatchMix(bool mix, Body body) {
  if (mix) {
    body(std::true_type{});
  } else {
    body(std::false_type{});
  }
}

template <typename ComputeHash>
inline void hashDecoded(
    bool mix,
    vector_size_t size,
    uint64_t* result,
    const DecodedVector& decoded,
    ComputeHash computeHash) {
  dispatchMix(mix, [&](auto mixTag) {
    constexpr bool kMix = decltype(mixTag)::value;
    if (decoded.mayHaveNulls()) {
      hashLoopWithNulls<kMix>(size, result, decoded, computeHash);
    } else {
      hashLoopNoNulls<kMix>(size, result, computeHash);
    }
  });
}

} // namespace

OptimizedVectorHasher::OptimizedVectorHasher(
    TypePtr type,
    column_index_t channel)
    : channel_(channel),
      type_(std::move(type)),
      typeKind_(type_->kind()),
      typeProvidesCustomComparison_(type_->providesCustomComparison()) {}

void OptimizedVectorHasher::decode(
    const BaseVector& vector,
    const SelectivityVector& rows) {
  VELOX_CHECK(
      type_->kindEquals(vector.type()),
      "Type mismatch: {} vs. {}",
      type_->toString(),
      vector.type()->toString());
  decoded_.decode(vector, rows);
}

void OptimizedVectorHasher::hash(bool mix, raw_vector<uint64_t>& result) {
  if (typeKind_ == TypeKind::UNKNOWN) {
    dispatchMix(mix, [&](auto mixTag) {
      broadcastHash<decltype(mixTag)::value>(
          decoded_.size(), result.data(), kNullHash);
    });
  } else {
    VELOX_DYNAMIC_TYPE_DISPATCH(hashValues, typeKind_, mix, result.data());
  }
}

void OptimizedVectorHasher::hash(
    const SelectivityVector& rows,
    bool mix,
    raw_vector<uint64_t>& result) {
  if (decoded_.size() == 0 || result.empty() || rows.isAllSelected()) {
    hash(mix, result);
    return;
  }

  const auto original = result;

  hash(mix, result);

  // The specialized hash() path computes values for the full decoded extent.
  // Restore rows that were not selected to match VectorHasher semantics.
  for (vector_size_t row = 0; row < result.size(); ++row) {
    if (!rows.isValid(row)) {
      result[row] = original[row];
    }
  }
}

template <TypeKind Kind>
void OptimizedVectorHasher::hashValues(bool mix, uint64_t* result) {
  using T = typename TypeTraits<Kind>::NativeType;
  if constexpr (
      Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
      Kind == TypeKind::MAP) {
    if (typeProvidesCustomComparison_) {
      hashTyped<true, Kind>(mix, result);
    } else {
      hashTyped<false, Kind>(mix, result);
    }
    return;
  }

  if (decoded_.isConstantMapping() || !decoded_.isIdentityMapping() ||
      typeProvidesCustomComparison_) {
    if (typeProvidesCustomComparison_) {
      hashTyped<true, Kind>(mix, result);
    } else {
      hashTyped<false, Kind>(mix, result);
    }
    return;
  }
  hashFlatValues<T>(mix, result);
}

template <bool typeProvidesCustomComparison, TypeKind Kind>
void OptimizedVectorHasher::hashTyped(bool mix, uint64_t* result) {
  const auto size = decoded_.size();

  // Constant column: compute the value once and broadcast.
  if (decoded_.isConstantMapping()) {
    const uint64_t hash = decoded_.isNullAt(0)
        ? kNullHash
        : hashOne<typeProvidesCustomComparison, Kind>(decoded_, 0);
    dispatchMix(mix, [&](auto mixTag) {
      broadcastHash<decltype(mixTag)::value>(size, result, hash);
    });
    return;
  }

  // Dictionary mapping more rows than its base: calculate the hashes for the
  // dictionary first, then scatter.
  if (!decoded_.isIdentityMapping() && size > decoded_.base()->size()) {
    const DecodedVector baseDecoded(*decoded_.base());
    const auto baseSize = decoded_.base()->size();
    dictionaryHashes_.resize(baseSize);
    const auto computeBaseHash = [&](vector_size_t i) {
      return hashOne<typeProvidesCustomComparison, Kind>(baseDecoded, i);
    };
    hashDecoded(
        false,
        baseSize,
        dictionaryHashes_.data(),
        baseDecoded,
        computeBaseHash);

    const auto* const indices = decoded_.indices();
    dispatchMix(mix, [&](auto mixTag) {
      constexpr bool kMix = decltype(mixTag)::value;
      if (decoded_.hasExtraNulls()) {
        scatterDictionaryHashesWithExtraNulls<kMix>(
            size, result, indices, decoded_.nulls(), dictionaryHashes_.data());
      } else {
        scatterDictionaryHashes<kMix>(
            size, result, indices, dictionaryHashes_.data());
      }
    });
    return;
  }

  // Generic fallback
  const auto computeHash = [&](vector_size_t i) {
    return hashOne<typeProvidesCustomComparison, Kind>(decoded_, i);
  };
  hashDecoded(mix, size, result, decoded_, computeHash);
}

template <typename T>
void OptimizedVectorHasher::hashFlatValues(bool mix, uint64_t* result) {
  if constexpr (std::is_void_v<T>) {
    VELOX_NYI();
  } else {
    const T* const values = decoded_.data<T>();
    const auto size = decoded_.size();
    const auto computeHash = [&](vector_size_t i) {
      if constexpr (std::is_floating_point_v<T>) {
        return util::floating_point::NaNAwareHash<T>()(values[i]);
      } else {
        return folly::hasher<T>()(values[i]);
      }
    };
    hashDecoded(mix, size, result, decoded_, computeHash);
  }
}

template <>
void OptimizedVectorHasher::hashFlatValues<bool>(bool mix, uint64_t* result) {
  const auto* const values = decoded_.data<uint64_t>();
  const auto* const nulls =
      decoded_.mayHaveNulls() ? decoded_.nulls() : nullptr;
  dispatchMix(mix, [&](auto mixTag) {
    scatterBoolHashes<decltype(mixTag)::value>(
        decoded_.size(), result, values, nulls);
  });
}

void OptimizedVectorHasher::hashPrecomputed(
    bool mix,
    raw_vector<uint64_t>& result) const {
  dispatchMix(mix, [&](auto mixTag) {
    broadcastHash<decltype(mixTag)::value>(
        result.size(), result.data(), precomputedHash_);
  });
}

uint64_t OptimizedVectorHasher::hashPrecomputed(bool mix, uint64_t previousHash)
    const {
  return mix ? bits::hashMix(previousHash, precomputedHash_) : precomputedHash_;
}

std::optional<uint64_t> OptimizedVectorHasher::hashConstant(
    bool mix,
    uint64_t previousHash) const {
  if (!decoded_.isConstantMapping() || decoded_.size() == 0) {
    return std::nullopt;
  }

  uint64_t hash;
  if (decoded_.isNullAt(0) || typeKind_ == TypeKind::UNKNOWN) {
    hash = kNullHash;
  } else if (typeProvidesCustomComparison_) {
    hash = VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
        hashOne, true, typeKind_, decoded_, 0);
  } else {
    hash = VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
        hashOne, false, typeKind_, decoded_, 0);
  }

  return mix ? bits::hashMix(previousHash, hash) : hash;
}

void OptimizedVectorHasher::precompute(const BaseVector& value) {
  if (value.isNullAt(0)) {
    precomputedHash_ = kNullHash;
    return;
  }

  decoded_.decode(value);
  if (typeKind_ == TypeKind::UNKNOWN) {
    precomputedHash_ = kNullHash;
    return;
  }

  if (typeProvidesCustomComparison_) {
    precomputedHash_ = VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
        hashOne, true, typeKind_, decoded_, 0);
  } else {
    precomputedHash_ = VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
        hashOne, false, typeKind_, decoded_, 0);
  }
}

} // namespace facebook::velox::exec
