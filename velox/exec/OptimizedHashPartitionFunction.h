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

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/OptimizedVectorHasher.h"

namespace facebook::velox::exec {

/// Maps hashes to partitions using range reduction. Visible for testing.
void rangeReduction(
    const uint64_t* hashes,
    uint32_t* partitions,
    vector_size_t size,
    uint32_t numPartitions);

/// Calculates partition numbers using OptimizedVectorHasher.
class OptimizedHashPartitionFunction : public HashPartitionFunctionBase {
 public:
  OptimizedHashPartitionFunction(
      bool localExchange,
      int numPartitions,
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<VectorPtr>& constValues = {});

  OptimizedHashPartitionFunction(
      const HashBitRange& hashBitRange,
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<VectorPtr>& constValues = {});

  ~OptimizedHashPartitionFunction() override = default;

  std::optional<uint32_t> partition(
      const RowVector& input,
      std::vector<uint32_t>& partitions) override;

  int numPartitions() const override {
    return numPartitions_;
  }

 private:
  void init(
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<VectorPtr>& constValues);

  const bool localExchange_;
  const int numPartitions_;
  const std::optional<HashBitRange> hashBitRange_ = std::nullopt;
  std::vector<std::unique_ptr<OptimizedVectorHasher>> hashers_;

  // Reusable memory.
  SelectivityVector rows_;
  raw_vector<uint64_t> hashes_;
};

} // namespace facebook::velox::exec
