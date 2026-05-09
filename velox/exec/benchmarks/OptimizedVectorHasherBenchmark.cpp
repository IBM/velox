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
#include <numeric>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/exec/OptimizedVectorHasher.h"
#include "velox/exec/VectorHasher.h"
#include "velox/type/HugeInt.h"
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

enum class NullMode {
  kNoNulls,
  kHalfNulls,
  kAllNulls,
};

enum class EncodingMode {
  kFlat,
  kDictionary,
  kConstant,
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
float makeValue<float>(vector_size_t row) {
  return static_cast<float>(row) * 1.25f - 1000.0f;
}

template <>
double makeValue<double>(vector_size_t row) {
  return static_cast<double>(row) * 1.25 - 1000.0;
}

template <>
int128_t makeValue<int128_t>(vector_size_t row) {
  return HugeInt::build(
      static_cast<int64_t>(row * 31),
      static_cast<uint64_t>(row * 1315423911ULL + 17));
}

template <>
StringView makeValue<StringView>(vector_size_t row) {
  thread_local std::array<char, 20> buffer;
  const auto length = 5 + row % 16;
  for (vector_size_t i = 0; i < length; ++i) {
    buffer[i] = 'a' + (row + i * 7) % 26;
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

template <typename T>
VectorPtr makeValuesVector(
    VectorMaker& vectorMaker,
    memory::MemoryPool* pool,
    NullMode nullMode,
    EncodingMode encodingMode,
    vector_size_t numValues,
    vector_size_t dictionarySize) {
  auto flat = vectorMaker.flatVector<T>(
      encodingMode == EncodingMode::kDictionary ? dictionarySize : numValues,
      [](vector_size_t row) { return makeValue<T>(row); },
      makeNulls(nullMode));

  switch (encodingMode) {
    case EncodingMode::kFlat:
      return flat;
    case EncodingMode::kDictionary: {
      auto indices = AlignedBuffer::allocate<vector_size_t>(numValues, pool);
      auto* rawIndices = indices->asMutable<vector_size_t>();
      for (vector_size_t i = 0; i < numValues; ++i) {
        rawIndices[i] = (numValues - i - 1) % dictionarySize;
      }
      return BaseVector::wrapInDictionary(
          BufferPtr(nullptr), indices, numValues, flat);
    }
    case EncodingMode::kConstant:
      if (nullMode == NullMode::kAllNulls) {
        return BaseVector::createNullConstant(
            CppToType<T>::create(), numValues, pool);
      }
      return BaseVector::wrapInConstant(numValues, 0, flat);
  }

  VELOX_UNREACHABLE();
}

template <typename Hasher>
struct HasherRunner;

template <>
struct HasherRunner<VectorHasher> {
  static std::unique_ptr<VectorHasher> create(const TypePtr& type) {
    return VectorHasher::create(type, 0);
  }
};

template <>
struct HasherRunner<OptimizedVectorHasher> {
  static std::unique_ptr<OptimizedVectorHasher> create(const TypePtr& type) {
    return OptimizedVectorHasher::create(type, 0);
  }
};

template <typename T, typename Hasher>
void runHashBenchmark(
    uint32_t iterations,
    NullMode nullMode,
    EncodingMode encodingMode,
    bool mix,
    vector_size_t size,
    vector_size_t dictionarySize) {
  folly::BenchmarkSuspender suspender;

  auto pool = memory::memoryManager()->addLeafPool();
  VectorMaker vectorMaker(pool.get());
  auto values = makeValuesVector<T>(
      vectorMaker, pool.get(), nullMode, encodingMode, size, dictionarySize);
  auto hasher = HasherRunner<Hasher>::create(CppToType<T>::create());
  raw_vector<uint64_t> hashes(size, pool.get());

  SelectivityVector rows(size);
  hasher->decode(*values, rows);
  if (mix) {
    std::iota(hashes.begin(), hashes.end(), 0);
  }

  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    hasher->hash(rows, mix, hashes);
    folly::doNotOptimizeAway(hashes.data());
  }
}

template <typename T>
void benchmarkVectorHasher(
    uint32_t iterations,
    NullMode nullMode,
    EncodingMode encodingMode,
    bool mix,
    vector_size_t size,
    vector_size_t dictionarySize) {
  runHashBenchmark<T, VectorHasher>(
      iterations, nullMode, encodingMode, mix, size, dictionarySize);
}

template <typename T>
void benchmarkOptimizedVectorHasher(
    uint32_t iterations,
    NullMode nullMode,
    EncodingMode encodingMode,
    bool mix,
    vector_size_t size,
    vector_size_t dictionarySize) {
  runHashBenchmark<T, OptimizedVectorHasher>(
      iterations, nullMode, encodingMode, mix, size, dictionarySize);
}

#define REGISTER_HASHER_PAIR(                                                  \
    T,                                                                         \
    TYPE_NAME,                                                                 \
    NULL_MODE,                                                                 \
    NULL_NAME,                                                                 \
    ENCODING_MODE,                                                             \
    ENCODING_NAME,                                                             \
    MIX,                                                                       \
    MIX_NAME,                                                                  \
    SIZE,                                                                      \
    DICTIONARY_SIZE)                                                           \
  BENCHMARK(TYPE_NAME##_##ENCODING_NAME##_##NULL_NAME##_##MIX_NAME, n) {       \
    benchmarkVectorHasher<T>(                                                  \
        n, NULL_MODE, ENCODING_MODE, MIX, SIZE, DICTIONARY_SIZE);              \
  }                                                                            \
  BENCHMARK_RELATIVE(                                                          \
      optimized_##TYPE_NAME##_##ENCODING_NAME##_##NULL_NAME##_##MIX_NAME, n) { \
    benchmarkOptimizedVectorHasher<T>(                                         \
        n, NULL_MODE, ENCODING_MODE, MIX, SIZE, DICTIONARY_SIZE);              \
  }                                                                            \
  BENCHMARK_DRAW_LINE();

#define REGISTER_HASHER_NULL_MODES( \
    T,                              \
    TYPE_NAME,                      \
    ENCODING_MODE,                  \
    ENCODING_NAME,                  \
    MIX,                            \
    MIX_NAME,                       \
    SIZE,                           \
    DICTIONARY_SIZE)                \
  REGISTER_HASHER_PAIR(             \
      T,                            \
      TYPE_NAME,                    \
      NullMode::kNoNulls,           \
      no_null,                      \
      ENCODING_MODE,                \
      ENCODING_NAME,                \
      MIX,                          \
      MIX_NAME,                     \
      SIZE,                         \
      DICTIONARY_SIZE)              \
  REGISTER_HASHER_PAIR(             \
      T,                            \
      TYPE_NAME,                    \
      NullMode::kHalfNulls,         \
      half_null,                    \
      ENCODING_MODE,                \
      ENCODING_NAME,                \
      MIX,                          \
      MIX_NAME,                     \
      SIZE,                         \
      DICTIONARY_SIZE)              \
  REGISTER_HASHER_PAIR(             \
      T,                            \
      TYPE_NAME,                    \
      NullMode::kAllNulls,          \
      all_null,                     \
      ENCODING_MODE,                \
      ENCODING_NAME,                \
      MIX,                          \
      MIX_NAME,                     \
      SIZE,                         \
      DICTIONARY_SIZE)

#define REGISTER_HASHER_NULL_MODES_CONSTANT(T, TYPE_NAME, MIX, MIX_NAME, SIZE) \
  REGISTER_HASHER_PAIR(                                                        \
      T,                                                                       \
      TYPE_NAME,                                                               \
      NullMode::kNoNulls,                                                      \
      no_null,                                                                 \
      EncodingMode::kConstant,                                                 \
      constant,                                                                \
      MIX,                                                                     \
      MIX_NAME,                                                                \
      SIZE,                                                                    \
      SIZE)                                                                    \
  REGISTER_HASHER_PAIR(                                                        \
      T,                                                                       \
      TYPE_NAME,                                                               \
      NullMode::kAllNulls,                                                     \
      all_null,                                                                \
      EncodingMode::kConstant,                                                 \
      constant,                                                                \
      MIX,                                                                     \
      MIX_NAME,                                                                \
      SIZE,                                                                    \
      SIZE)

#define REGISTER_HASHER_SIZES(                                 \
    T, TYPE_NAME, ENCODING_MODE, ENCODING_NAME, MIX, MIX_NAME) \
  REGISTER_HASHER_NULL_MODES(                                  \
      T, TYPE_NAME, ENCODING_MODE, ENCODING_NAME, MIX, MIX_NAME, 10000, 10000)

#define REGISTER_HASHER_SIZES_CONSTANT(T, TYPE_NAME, MIX, MIX_NAME) \
  REGISTER_HASHER_NULL_MODES_CONSTANT(T, TYPE_NAME, MIX, MIX_NAME, 10000)

#define REGISTER_HASHER_SIZES_DICTIONARY_FOR_PERCENT(         \
    T, TYPE_NAME, MIX, MIX_NAME, SIZE, PERCENT, PERCENT_NAME) \
  REGISTER_HASHER_NULL_MODES(                                 \
      T,                                                      \
      TYPE_NAME,                                              \
      EncodingMode::kDictionary,                              \
      dictionary_##PERCENT_NAME,                              \
      MIX,                                                    \
      MIX_NAME,                                               \
      SIZE,                                                   \
      SIZE* PERCENT / 100)

#define REGISTER_HASHER_SIZES_DICTIONARY(T, TYPE_NAME, MIX, MIX_NAME) \
  REGISTER_HASHER_SIZES_DICTIONARY_FOR_PERCENT(                       \
      T, TYPE_NAME, MIX, MIX_NAME, 10000, 80, 80pct)                  \
  REGISTER_HASHER_SIZES_DICTIONARY_FOR_PERCENT(                       \
      T, TYPE_NAME, MIX, MIX_NAME, 10000, 60, 60pct)                  \
  REGISTER_HASHER_SIZES_DICTIONARY_FOR_PERCENT(                       \
      T, TYPE_NAME, MIX, MIX_NAME, 10000, 40, 40pct)                  \
  REGISTER_HASHER_SIZES_DICTIONARY_FOR_PERCENT(                       \
      T, TYPE_NAME, MIX, MIX_NAME, 10000, 20, 20pct)                  \
  REGISTER_HASHER_SIZES_DICTIONARY_FOR_PERCENT(                       \
      T, TYPE_NAME, MIX, MIX_NAME, 10000, 5, 5pct)

#define REGISTER_HASHER_ENCODINGS(T, TYPE_NAME, MIX, MIX_NAME)  \
  REGISTER_HASHER_SIZES(                                        \
      T, TYPE_NAME, EncodingMode::kFlat, flat, MIX, MIX_NAME)   \
  REGISTER_HASHER_SIZES_DICTIONARY(T, TYPE_NAME, MIX, MIX_NAME) \
  REGISTER_HASHER_SIZES_CONSTANT(T, TYPE_NAME, MIX, MIX_NAME)

#define REGISTER_HASHER_TYPE(T, TYPE_NAME)               \
  REGISTER_HASHER_ENCODINGS(T, TYPE_NAME, false, no_mix) \
  REGISTER_HASHER_ENCODINGS(T, TYPE_NAME, true, mix)

REGISTER_HASHER_TYPE(bool, boolean)
REGISTER_HASHER_TYPE(int8_t, tinyint)
REGISTER_HASHER_TYPE(int16_t, smallint)
REGISTER_HASHER_TYPE(int32_t, integer)
REGISTER_HASHER_TYPE(int64_t, bigint)
REGISTER_HASHER_TYPE(int128_t, hugeint)
REGISTER_HASHER_TYPE(float, real)
REGISTER_HASHER_TYPE(double, double)
REGISTER_HASHER_TYPE(StringView, varchar)

#undef REGISTER_HASHER_TYPE
#undef REGISTER_HASHER_SIZES_DICTIONARY
#undef REGISTER_HASHER_SIZES_DICTIONARY_FOR_PERCENT
#undef REGISTER_HASHER_SIZES
#undef REGISTER_HASHER_NULL_MODES
#undef REGISTER_HASHER_PAIR

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
