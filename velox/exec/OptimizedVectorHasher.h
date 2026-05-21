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

#include "velox/common/memory/RawVector.h"
#include "velox/exec/Operator.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

class OptimizedVectorHasher {
 public:
  OptimizedVectorHasher(TypePtr type, column_index_t channel);

  static std::unique_ptr<OptimizedVectorHasher> create(
      TypePtr type,
      column_index_t channel) {
    return std::make_unique<OptimizedVectorHasher>(std::move(type), channel);
  }

  column_index_t channel() const {
    return channel_;
  }

  // Decodes the 'vector' in preparation for calling hash() or
  // computeValueIds(). The decoded vector can be accessed via decodedVector()
  // getter.
  void decode(const BaseVector& vector, const SelectivityVector& rows);

  void hash(bool mix, raw_vector<uint64_t>& result);

  void
  hash(const SelectivityVector& rows, bool mix, raw_vector<uint64_t>& result);

  void hashPrecomputed(bool mix, raw_vector<uint64_t>& result) const;

  // Computes one hash from a precomputed single value.
  uint64_t hashPrecomputed(bool mix, uint64_t previousHash) const;

  // Computes one hash when the decoded vector has constant mapping.
  std::optional<uint64_t> hashConstant(bool mix, uint64_t previousHash) const;

  void precompute(const BaseVector& value);

  static constexpr uint64_t kNullHash = BaseVector::kNullHash;

  template <TypeKind Kind>
  void hashValues(bool mix, uint64_t* result);

 private:
  template <bool typeProvidesCustomComparison, TypeKind Kind>
  void hashTyped(bool mix, uint64_t* result);

  template <typename T>
  void hashFlatValues(bool mix, uint64_t* result);

  const column_index_t channel_;
  const TypePtr type_;
  const TypeKind typeKind_;
  const bool typeProvidesCustomComparison_;

  DecodedVector decoded_;
  raw_vector<uint64_t> dictionaryHashes_;
  uint64_t precomputedHash_{0};
};

} // namespace facebook::velox::exec
