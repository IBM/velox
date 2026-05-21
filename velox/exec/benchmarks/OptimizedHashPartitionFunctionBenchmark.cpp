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

#include <array>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/exec/OptimizedHashPartitionFunction.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

// Add the following definitions to allow Clion runs.
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

constexpr vector_size_t kSize = 10'000;
constexpr vector_size_t kDictionarySize = kSize / 5;

enum class FunctionKind {
  kNormal,
  kOptimized,
};

enum class EncodingMode {
  kFlat,
  kDictionary,
  kConstant,
};

enum class NullMode {
  kNoNulls,
  kHalfNulls,
  kAllNulls,
};

enum class PartitionMode {
  kRemote,
  kLocalExchange,
  kHashBitRangeFirst8,
  kHashBitRangeLast8,
};

template <typename T>
T makeValue(vector_size_t row) {
  return static_cast<T>((row * 8191) ^ (row >> 3));
}

template <>
bool makeValue<bool>(vector_size_t row) {
  return (row & 1) == 0;
}

template <>
StringView makeValue<StringView>(vector_size_t row) {
  thread_local std::array<char, 20> buffer;
  const auto length = 5 + row % 16;
  for (vector_size_t index = 0; index < length; ++index) {
    buffer[index] = 'a' + (row + index * 7) % 26;
  }
  return StringView(buffer.data(), length);
}

std::function<bool(vector_size_t)> makeNulls(NullMode nullMode) {
  switch (nullMode) {
    case NullMode::kNoNulls:
      return nullptr;
    case NullMode::kHalfNulls:
      return [](vector_size_t row) { return (row & 1) == 0; };
    case NullMode::kAllNulls:
      return [](vector_size_t /*row*/) { return true; };
  }

  VELOX_UNREACHABLE();
}

VectorPtr wrapInDictionary(
    const VectorPtr& base,
    vector_size_t size,
    memory::MemoryPool* pool,
    NullMode nullMode = NullMode::kNoNulls) {
  auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  const auto baseSize = base->size();
  for (vector_size_t row = 0; row < size; ++row) {
    rawIndices[row] = (size - row - 1) % baseSize;
  }

  BufferPtr nulls;
  if (nullMode == NullMode::kHalfNulls) {
    nulls = AlignedBuffer::allocate<bool>(size, pool);
    auto* rawNulls = nulls->asMutable<uint64_t>();
    bits::fillBits(rawNulls, 0, size, bits::kNotNull);
    for (vector_size_t row = 0; row < size; row += 2) {
      bits::setNull(rawNulls, row);
    }
  } else if (nullMode == NullMode::kAllNulls) {
    nulls = AlignedBuffer::allocate<bool>(size, pool);
    auto* rawNulls = nulls->asMutable<uint64_t>();
    bits::fillBits(rawNulls, 0, size, bits::kNull);
  }

  return BaseVector::wrapInDictionary(nulls, indices, size, base);
}

template <typename T>
VectorPtr makeValuesVector(
    VectorMaker& vectorMaker,
    memory::MemoryPool* pool,
    EncodingMode encodingMode,
    NullMode nullMode,
    vector_size_t size) {
  const auto flatSize =
      encodingMode == EncodingMode::kDictionary ? kDictionarySize : size;
  auto flat = vectorMaker.flatVector<T>(
      flatSize,
      [](vector_size_t row) { return makeValue<T>(row); },
      makeNulls(nullMode));

  switch (encodingMode) {
    case EncodingMode::kFlat:
      return flat;
    case EncodingMode::kDictionary:
      return wrapInDictionary(flat, size, pool);
    case EncodingMode::kConstant:
      if (nullMode == NullMode::kAllNulls) {
        return BaseVector::createNullConstant(
            CppToType<T>::create(), size, pool);
      }
      if (nullMode == NullMode::kHalfNulls) {
        auto constant = BaseVector::wrapInConstant(size, 1, flat);
        // ConstantVector has one nullness for all logical rows. Use a
        // dictionary wrapper to express alternating nulls while keeping the
        // repeated-value payload constant.
        return wrapInDictionary(constant, size, pool, nullMode);
      }
      return BaseVector::wrapInConstant(size, 0, flat);
  }

  VELOX_UNREACHABLE();
}

template <FunctionKind Kind>
std::unique_ptr<HashPartitionFunctionBase> makePartitionFunction(
    PartitionMode partitionMode,
    const RowTypePtr& inputType,
    int numPartitions) {
  switch (partitionMode) {
    case PartitionMode::kRemote:
      if constexpr (Kind == FunctionKind::kNormal) {
        return std::make_unique<HashPartitionFunction>(
            false, numPartitions, inputType, std::vector<column_index_t>{0});
      } else {
        return std::make_unique<OptimizedHashPartitionFunction>(
            false, numPartitions, inputType, std::vector<column_index_t>{0});
      }
    case PartitionMode::kLocalExchange:
      if constexpr (Kind == FunctionKind::kNormal) {
        return std::make_unique<HashPartitionFunction>(
            true, numPartitions, inputType, std::vector<column_index_t>{0});
      } else {
        return std::make_unique<OptimizedHashPartitionFunction>(
            true, numPartitions, inputType, std::vector<column_index_t>{0});
      }
    case PartitionMode::kHashBitRangeFirst8:
      if constexpr (Kind == FunctionKind::kNormal) {
        return std::make_unique<HashPartitionFunction>(
            HashBitRange{0, 8}, inputType, std::vector<column_index_t>{0});
      } else {
        return std::make_unique<OptimizedHashPartitionFunction>(
            HashBitRange{0, 8}, inputType, std::vector<column_index_t>{0});
      }
    case PartitionMode::kHashBitRangeLast8:
      if constexpr (Kind == FunctionKind::kNormal) {
        return std::make_unique<HashPartitionFunction>(
            HashBitRange{56, 64}, inputType, std::vector<column_index_t>{0});
      } else {
        return std::make_unique<OptimizedHashPartitionFunction>(
            HashBitRange{56, 64}, inputType, std::vector<column_index_t>{0});
      }
  }

  VELOX_UNREACHABLE();
}

void normalRangeReduction(
    const uint64_t* hashes,
    uint32_t* partitions,
    int size,
    uint32_t numPartitions) {
  for (int index = 0; index < size; ++index) {
    partitions[index] = hashes[index] % numPartitions;
  }
}

template <FunctionKind Kind>
void runRangeReductionBenchmark(uint32_t iterations, uint32_t numPartitions) {
  folly::BenchmarkSuspender suspender;

  std::vector<uint64_t> hashes(kSize);
  std::vector<uint32_t> partitions(kSize);
  for (vector_size_t row = 0; row < kSize; ++row) {
    hashes[row] = (static_cast<uint64_t>(row * 8191) << 32) ^
        static_cast<uint64_t>(row * 1315423911ULL + 17);
  }

  suspender.dismiss();

  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    if constexpr (Kind == FunctionKind::kNormal) {
      normalRangeReduction(
          hashes.data(), partitions.data(), kSize, numPartitions);
    } else {
      rangeReduction(hashes.data(), partitions.data(), kSize, numPartitions);
    }
    folly::doNotOptimizeAway(partitions.data());
  }
}

template <typename T, FunctionKind Kind>
void runPartitionBenchmark(
    uint32_t iterations,
    PartitionMode partitionMode,
    EncodingMode encodingMode,
    NullMode nullMode,
    int numPartitions) {
  folly::BenchmarkSuspender suspender;

  auto pool = memory::memoryManager()->addLeafPool();
  VectorMaker vectorMaker(pool.get());
  auto values = makeValuesVector<T>(
      vectorMaker, pool.get(), encodingMode, nullMode, kSize);
  auto input = vectorMaker.rowVector({values});
  auto partitionFunction = makePartitionFunction<Kind>(
      partitionMode, asRowType(input->type()), numPartitions);
  std::vector<uint32_t> partitions;

  suspender.dismiss();

  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    std::optional<uint32_t> singlePartition =
        partitionFunction->partition(*input, partitions);

    folly::doNotOptimizeAway(partitions.data());
  }
}

template <typename T>
void benchmarkNormalHashPartitionFunction(
    uint32_t iterations,
    PartitionMode partitionMode,
    EncodingMode encodingMode,
    NullMode nullMode,
    int numPartitions) {
  runPartitionBenchmark<T, FunctionKind::kNormal>(
      iterations, partitionMode, encodingMode, nullMode, numPartitions);
}

template <typename T>
void benchmarkOptimizedHashPartitionFunction(
    uint32_t iterations,
    PartitionMode partitionMode,
    EncodingMode encodingMode,
    NullMode nullMode,
    int numPartitions) {
  runPartitionBenchmark<T, FunctionKind::kOptimized>(
      iterations, partitionMode, encodingMode, nullMode, numPartitions);
}

#define REGISTER_PARTITION_PAIR(                                                                                  \
    T,                                                                                                            \
    TYPE_NAME,                                                                                                    \
    PARTITION_MODE,                                                                                               \
    PARTITION_NAME,                                                                                               \
    NUM_PARTITIONS,                                                                                               \
    NUM_PARTITIONS_NAME,                                                                                          \
    ENCODING_MODE,                                                                                                \
    ENCODING_NAME,                                                                                                \
    NULL_MODE,                                                                                                    \
    NULL_NAME)                                                                                                    \
  BENCHMARK(                                                                                                      \
      partition_##TYPE_NAME##_##PARTITION_NAME##_##NUM_PARTITIONS_NAME##_##ENCODING_NAME##_##NULL_NAME,           \
      iterations) {                                                                                               \
    benchmarkNormalHashPartitionFunction<T>(                                                                      \
        iterations, PARTITION_MODE, ENCODING_MODE, NULL_MODE, NUM_PARTITIONS);                                    \
  }                                                                                                               \
  BENCHMARK_RELATIVE(                                                                                             \
      optimized_partition_##TYPE_NAME##_##PARTITION_NAME##_##NUM_PARTITIONS_NAME##_##ENCODING_NAME##_##NULL_NAME, \
      iterations) {                                                                                               \
    benchmarkOptimizedHashPartitionFunction<T>(                                                                   \
        iterations, PARTITION_MODE, ENCODING_MODE, NULL_MODE, NUM_PARTITIONS);                                    \
  }                                                                                                               \
  BENCHMARK_DRAW_LINE();

#define REGISTER_PARTITION_NULL_MODES( \
    T,                                 \
    TYPE_NAME,                         \
    PARTITION_MODE,                    \
    PARTITION_NAME,                    \
    NUM_PARTITIONS,                    \
    NUM_PARTITIONS_NAME,               \
    ENCODING_MODE,                     \
    ENCODING_NAME)                     \
  REGISTER_PARTITION_PAIR(             \
      T,                               \
      TYPE_NAME,                       \
      PARTITION_MODE,                  \
      PARTITION_NAME,                  \
      NUM_PARTITIONS,                  \
      NUM_PARTITIONS_NAME,             \
      ENCODING_MODE,                   \
      ENCODING_NAME,                   \
      NullMode::kNoNulls,              \
      no_null)                         \
  REGISTER_PARTITION_PAIR(             \
      T,                               \
      TYPE_NAME,                       \
      PARTITION_MODE,                  \
      PARTITION_NAME,                  \
      NUM_PARTITIONS,                  \
      NUM_PARTITIONS_NAME,             \
      ENCODING_MODE,                   \
      ENCODING_NAME,                   \
      NullMode::kHalfNulls,            \
      half_null)                       \
  REGISTER_PARTITION_PAIR(             \
      T,                               \
      TYPE_NAME,                       \
      PARTITION_MODE,                  \
      PARTITION_NAME,                  \
      NUM_PARTITIONS,                  \
      NUM_PARTITIONS_NAME,             \
      ENCODING_MODE,                   \
      ENCODING_NAME,                   \
      NullMode::kAllNulls,             \
      all_null)

#define REGISTER_PARTITION_ENCODINGS( \
    T,                                \
    TYPE_NAME,                        \
    PARTITION_MODE,                   \
    PARTITION_NAME,                   \
    NUM_PARTITIONS,                   \
    NUM_PARTITIONS_NAME)              \
  REGISTER_PARTITION_NULL_MODES(      \
      T,                              \
      TYPE_NAME,                      \
      PARTITION_MODE,                 \
      PARTITION_NAME,                 \
      NUM_PARTITIONS,                 \
      NUM_PARTITIONS_NAME,            \
      EncodingMode::kFlat,            \
      flat)                           \
  REGISTER_PARTITION_NULL_MODES(      \
      T,                              \
      TYPE_NAME,                      \
      PARTITION_MODE,                 \
      PARTITION_NAME,                 \
      NUM_PARTITIONS,                 \
      NUM_PARTITIONS_NAME,            \
      EncodingMode::kDictionary,      \
      dictionary)                     \
  REGISTER_PARTITION_NULL_MODES(      \
      T,                              \
      TYPE_NAME,                      \
      PARTITION_MODE,                 \
      PARTITION_NAME,                 \
      NUM_PARTITIONS,                 \
      NUM_PARTITIONS_NAME,            \
      EncodingMode::kConstant,        \
      constant)

#define REGISTER_PARTITION_COUNTS(                                \
    T, TYPE_NAME, PARTITION_MODE, PARTITION_NAME)                 \
  REGISTER_PARTITION_ENCODINGS(                                   \
      T, TYPE_NAME, PARTITION_MODE, PARTITION_NAME, 1, p1)        \
  REGISTER_PARTITION_ENCODINGS(                                   \
      T, TYPE_NAME, PARTITION_MODE, PARTITION_NAME, 4, p4)        \
  REGISTER_PARTITION_ENCODINGS(                                   \
      T, TYPE_NAME, PARTITION_MODE, PARTITION_NAME, 16, p16)      \
  REGISTER_PARTITION_ENCODINGS(                                   \
      T, TYPE_NAME, PARTITION_MODE, PARTITION_NAME, 100, p100)    \
  REGISTER_PARTITION_ENCODINGS(                                   \
      T, TYPE_NAME, PARTITION_MODE, PARTITION_NAME, 1'000, p1000) \
  REGISTER_PARTITION_ENCODINGS(                                   \
      T, TYPE_NAME, PARTITION_MODE, PARTITION_NAME, 1'024, p1024)

#define REGISTER_PARTITION_MODES(T, TYPE_NAME)                            \
  REGISTER_PARTITION_COUNTS(T, TYPE_NAME, PartitionMode::kRemote, remote) \
  REGISTER_PARTITION_COUNTS(                                              \
      T, TYPE_NAME, PartitionMode::kLocalExchange, local_exchange)        \
  REGISTER_PARTITION_ENCODINGS(                                           \
      T,                                                                  \
      TYPE_NAME,                                                          \
      PartitionMode::kHashBitRangeFirst8,                                 \
      hashbits_0_8,                                                       \
      0,                                                                  \
      hashbits)                                                           \
  REGISTER_PARTITION_ENCODINGS(                                           \
      T,                                                                  \
      TYPE_NAME,                                                          \
      PartitionMode::kHashBitRangeLast8,                                  \
      hashbits_last_8,                                                    \
      0,                                                                  \
      hashbits)

REGISTER_PARTITION_MODES(bool, bool)
REGISTER_PARTITION_MODES(int8_t, tinyint)
REGISTER_PARTITION_MODES(int16_t, smallint)
REGISTER_PARTITION_MODES(int32_t, integer)
REGISTER_PARTITION_MODES(int64_t, bigint)
REGISTER_PARTITION_MODES(StringView, varchar)

#define REGISTER_RANGE_REDUCTION_PAIR(NUM_PARTITIONS, NUM_PARTITIONS_NAME) \
  BENCHMARK(normal_range_reduction_##NUM_PARTITIONS_NAME, iterations) {    \
    runRangeReductionBenchmark<FunctionKind::kNormal>(                     \
        iterations, NUM_PARTITIONS);                                       \
  }                                                                        \
  BENCHMARK_RELATIVE(                                                      \
      optimized_range_reduction_##NUM_PARTITIONS_NAME, iterations) {       \
    runRangeReductionBenchmark<FunctionKind::kOptimized>(                  \
        iterations, NUM_PARTITIONS);                                       \
  }                                                                        \
  BENCHMARK_DRAW_LINE();

REGISTER_RANGE_REDUCTION_PAIR(1, p1)
REGISTER_RANGE_REDUCTION_PAIR(4, p4)
REGISTER_RANGE_REDUCTION_PAIR(16, p16)
REGISTER_RANGE_REDUCTION_PAIR(100, p100)
REGISTER_RANGE_REDUCTION_PAIR(1'000, p1000)
REGISTER_RANGE_REDUCTION_PAIR(1'024, p1024)

#undef REGISTER_PARTITION_MODES
#undef REGISTER_PARTITION_COUNTS
#undef REGISTER_PARTITION_ENCODINGS
#undef REGISTER_PARTITION_NULL_MODES
#undef REGISTER_PARTITION_PAIR
#undef REGISTER_RANGE_REDUCTION_PAIR

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
