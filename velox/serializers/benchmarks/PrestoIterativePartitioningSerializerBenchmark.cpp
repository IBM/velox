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
#include <string>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/serializers/PrestoIterativePartitioningSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

// Add the following definitions to allow Clion runs
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

using namespace facebook::velox;
using namespace facebook::velox::serializer::presto;

constexpr int64_t kBufferSize = 2 * 1024 * 1024;

namespace {

class PrestoIterativePartitioningSerializerBenchmark
    : public test::VectorTestBase {
 public:
  /// Creates a flat vector of type T with deterministic null pattern.
  /// Rows where (row % 100) < nullPct are null.
  template <typename T>
  VectorPtr makeFlatColumnOfType(vector_size_t size, int32_t nullPct) {
    if (nullPct == 0) {
      return makeFlatVector<T>(
          size, [](auto row) { return static_cast<T>(row); });
    }
    return makeFlatVector<T>(
        size,
        [](auto row) { return static_cast<T>(row); },
        [nullPct](auto row) { return (row % 100) < nullPct; });
  }

  VectorPtr makeVariableWidthFlatColumn(
      vector_size_t size,
      const TypePtr& type,
      int32_t nullPct) {
    auto valueAt = [](auto row) {
      return row % 2 == 0 ? fmt::format("short{}", row)
                          : fmt::format("long_string_value_{}_suffix", row);
    };
    if (nullPct == 0) {
      return makeFlatVector<std::string>(size, valueAt, nullptr, type);
    }
    return makeFlatVector<std::string>(
        size,
        valueAt,
        [nullPct](auto row) { return (row % 100) < nullPct; },
        type);
  }

  /// Creates a flat vector of the given TypeKind with the given null ratio.
  VectorPtr
  makeFlatColumn(vector_size_t size, TypeKind colKind, int32_t nullPct) {
    switch (colKind) {
      case TypeKind::BOOLEAN:
        return makeFlatColumnOfType<bool>(size, nullPct);
      case TypeKind::INTEGER:
        return makeFlatColumnOfType<int32_t>(size, nullPct);
      case TypeKind::BIGINT:
        return makeFlatColumnOfType<int64_t>(size, nullPct);
      case TypeKind::HUGEINT:
        return makeFlatColumnOfType<int128_t>(size, nullPct);
      case TypeKind::VARCHAR:
        return makeVariableWidthFlatColumn(size, VARCHAR(), nullPct);
      case TypeKind::VARBINARY:
        return makeVariableWidthFlatColumn(size, VARBINARY(), nullPct);
      default:
        VELOX_UNSUPPORTED(
            "Unsupported TypeKind: {}", TypeKindName::toName(colKind));
    }
  }

  VectorPtr
  makeConstantColumn(vector_size_t size, TypeKind colKind, bool nullConstant) {
    if (nullConstant) {
      return makeNullConstant(colKind, size);
    }
    switch (colKind) {
      case TypeKind::BOOLEAN:
        return makeConstant<bool>(true, size);
      case TypeKind::INTEGER:
        return makeConstant<int32_t>(42, size);
      case TypeKind::BIGINT:
        return makeConstant<int64_t>(1000, size);
      case TypeKind::HUGEINT:
        return makeConstant<int128_t>(10000, size);
      case TypeKind::VARCHAR:
        return makeConstant<std::string>("constant_string_value", size);
      case TypeKind::VARBINARY:
        return makeConstant<std::string>(
            std::string("\x01\x02\x03\x04", 4), size, VARBINARY());
      default:
        VELOX_UNSUPPORTED(
            "Unsupported TypeKind: {}", TypeKindName::toName(colKind));
    }
  }

  /// Builds a one-column input RowVector whose column is an N-level nested
  /// ROW with a BIGINT leaf. nestingLevel == 1 → outer column is ROW(BIGINT);
  /// each additional level wraps the previous type as the only field of a new
  /// ROW. Values are the row index at the leaf; no nulls are placed.
  RowVectorPtr makeNestedRowInput(vector_size_t size, uint32_t nestingLevel) {
    VectorPtr current = makeFlatVector<int64_t>(
        size, [](auto row) { return static_cast<int64_t>(row); });
    for (uint32_t i = 0; i < nestingLevel; ++i) {
      current = makeRowVector({i == 0 ? "a" : "r"}, {current});
    }
    return makeRowVector({"v"}, {current});
  }

  /// Like makeNestedRowInput but places nulls on every ROW level and on the
  /// leaf. Each level uses a distinct stride so combined parent-null masks
  /// vary across levels, exercising the AND-and-extract path. Stride is
  /// chosen to give roughly nullPct% nulls at each level.
  RowVectorPtr makeNestedRowInputWithNulls(
      vector_size_t size,
      uint32_t nestingLevel,
      int32_t nullPct) {
    auto strideFor = [&](uint32_t levelIdx) {
      // Different stride per level to avoid coincidental alignment.
      const int32_t base = std::max<int32_t>(2, 100 / std::max(1, nullPct));
      return base + static_cast<int32_t>(levelIdx);
    };
    VectorPtr current = makeFlatVector<int64_t>(
        size,
        [](auto row) { return static_cast<int64_t>(row); },
        nullEvery(strideFor(0)));
    for (uint32_t i = 0; i < nestingLevel; ++i) {
      current = makeRowVector(
          {i == 0 ? "a" : "r"}, {current}, nullEvery(strideFor(i + 1)));
    }
    return makeRowVector({"v"}, {current});
  }

  /// Creates a dictionary column over a 1'024-value null-free base with the
  /// given wrapper null ratio.
  VectorPtr
  makeDictionaryColumn(vector_size_t size, TypeKind colKind, int32_t nullPct) {
    const vector_size_t baseSize = std::min<vector_size_t>(size, 1'024);
    auto base = makeFlatColumn(baseSize, colKind, 0);
    auto indices =
        makeIndices(size, [baseSize](auto row) { return row % baseSize; });
    BufferPtr nulls =
        nullPct == 0 ? nullptr : makeNulls(size, [nullPct](auto row) {
          return (row % 100) < nullPct;
        });
    return BaseVector::wrapInDictionary(
        std::move(nulls), std::move(indices), size, std::move(base));
  }

  /// Creates a RowVector with numCols columns of the given TypeKind.
  RowVectorPtr makeInput(
      vector_size_t size,
      VectorEncoding::Simple encoding,
      TypeKind colKind,
      uint32_t numCols,
      int32_t nullPct,
      bool nullConstant = false) {
    std::vector<std::string> names;
    std::vector<VectorPtr> children;
    names.reserve(numCols);
    children.reserve(numCols);
    for (uint32_t i = 0; i < numCols; ++i) {
      names.push_back(fmt::format("c{}", i));
    }
    switch (encoding) {
      case VectorEncoding::Simple::FLAT: {
        for (uint32_t i = 0; i < numCols; ++i) {
          children.push_back(makeFlatColumn(size, colKind, nullPct));
        }
        break;
      }
      case VectorEncoding::Simple::CONSTANT: {
        for (uint32_t i = 0; i < numCols; ++i) {
          children.push_back(makeConstantColumn(size, colKind, nullConstant));
        }
        break;
      }
      case VectorEncoding::Simple::DICTIONARY: {
        for (uint32_t i = 0; i < numCols; ++i) {
          children.push_back(makeDictionaryColumn(size, colKind, nullPct));
        }
        break;
      }
      default:
        VELOX_UNSUPPORTED("Unsupported encoding: {}", encoding);
    }
    return makeRowVector(names, children);
  }

  std::vector<uint32_t> makePartitions(
      vector_size_t size,
      uint32_t numPartitions) {
    std::vector<uint32_t> partitions(size);
    for (vector_size_t i = 0; i < size; ++i) {
      partitions[i] = i % numPartitions;
    }
    return partitions;
  }

  std::unique_ptr<PrestoIterativePartitioningSerializer> makeSerializer(
      const RowTypePtr& type,
      uint32_t numPartitions) {
    SerdeOpts opts;
    return std::make_unique<PrestoIterativePartitioningSerializer>(
        type, numPartitions, opts, pool_.get());
  }
};

} // namespace

/// Single benchmark function parameterized by (encoding, colKind, numCols,
/// nullPct, nullConstant, numPartitions). Registered via BENCHMARK_NAMED_PARAM
/// below.
///
/// All runs use 10'000 rows. Setup (input creation, serializer construction,
/// append) is excluded from the measured time.
void benchmarkFlush(
    VectorEncoding::Simple encoding,
    TypeKind colKind,
    uint32_t numCols,
    int32_t nullPct,
    bool nullConstant,
    uint32_t numPartitions) {
  folly::BenchmarkSuspender suspender;
  PrestoIterativePartitioningSerializerBenchmark benchmark;
  auto input = benchmark.makeInput(
      10'000, encoding, colKind, numCols, nullPct, nullConstant);
  auto parts = benchmark.makePartitions(10'000, numPartitions);
  auto serializer = benchmark.makeSerializer(
      std::static_pointer_cast<const RowType>(input->type()), numPartitions);

  while (serializer->bytesBuffered() < kBufferSize) {
    // Partitioning rearranges the appended vector in place, so each round
    // must append a fresh copy of the input.
    serializer->append(
        std::static_pointer_cast<RowVector>(
            input->testingCopyPreserveEncodings()),
        parts);
  }

  suspender.dismiss();

  auto result = serializer->flush();
  folly::doNotOptimizeAway(result);
}

void benchmarkFlushFlat(
    uint32_t /* iters */,
    TypeKind colKind,
    uint32_t numCols,
    int32_t nullPct,
    uint32_t numPartitions) {
  benchmarkFlush(
      VectorEncoding::Simple::FLAT,
      colKind,
      numCols,
      nullPct,
      false,
      numPartitions);
}

void benchmarkFlushDictionary(
    uint32_t /* iters */,
    TypeKind colKind,
    uint32_t numCols,
    int32_t nullPct,
    uint32_t numPartitions) {
  benchmarkFlush(
      VectorEncoding::Simple::DICTIONARY,
      colKind,
      numCols,
      nullPct,
      false,
      numPartitions);
}

void benchmarkFlushConstant(
    uint32_t /* iters */,
    TypeKind colKind,
    uint32_t numCols,
    bool nullConstant,
    uint32_t numPartitions) {
  benchmarkFlush(
      VectorEncoding::Simple::CONSTANT,
      colKind,
      numCols,
      0,
      nullConstant,
      numPartitions);
}

/// Single benchmark function for N-level nested ROW columns. The input is a
/// one-column RowVector whose column is itself a nested ROW with a BIGINT
/// leaf at depth `nestingLevel`. No nulls are placed at any level.
void benchmarkFlushNestedRow(
    uint32_t /* iters */,
    uint32_t nestingLevel,
    uint32_t numPartitions) {
  folly::BenchmarkSuspender suspender;
  PrestoIterativePartitioningSerializerBenchmark benchmark;

  auto input = benchmark.makeNestedRowInput(10'000, nestingLevel);
  auto rowType = std::static_pointer_cast<const RowType>(input->type());
  auto parts = benchmark.makePartitions(10'000, numPartitions);
  auto serializer = benchmark.makeSerializer(rowType, numPartitions);

  while (serializer->bytesBuffered() < kBufferSize) {
    // Partitioning rearranges the appended vector in place, so each round
    // must append a fresh copy of the input.
    serializer->append(
        std::static_pointer_cast<RowVector>(
            input->testingCopyPreserveEncodings()),
        parts);
  }

  suspender.dismiss();

  auto result = serializer->flush();
  folly::doNotOptimizeAway(result);
}

/// Benchmark variant where every ROW level and the leaf carry nulls.
/// Exercises the AND/extract compaction path at flush time.
void benchmarkFlushNestedRowWithNulls(
    uint32_t /* iters */,
    uint32_t nestingLevel,
    int32_t nullPct,
    uint32_t numPartitions) {
  folly::BenchmarkSuspender suspender;
  PrestoIterativePartitioningSerializerBenchmark benchmark;

  auto input =
      benchmark.makeNestedRowInputWithNulls(10'000, nestingLevel, nullPct);
  auto rowType = std::static_pointer_cast<const RowType>(input->type());
  auto parts = benchmark.makePartitions(10'000, numPartitions);
  auto serializer = benchmark.makeSerializer(rowType, numPartitions);

  while (serializer->bytesBuffered() < kBufferSize) {
    // Partitioning rearranges the appended vector in place, so each round
    // must append a fresh copy of the input.
    serializer->append(
        std::static_pointer_cast<RowVector>(
            input->testingCopyPreserveEncodings()),
        parts);
  }

  suspender.dismiss();

  auto result = serializer->flush();
  folly::doNotOptimizeAway(result);
}

// clang-format off
// Dimensions:
//   col type:       {bool, int, bigint, hugeint}
//   num cols:       {1, 4, 16, 64}
//   null pct:       {0, 25, 50, 75, 100}
//   num partitions: {1, 4, 16, 64, 256, 1024}
//
// Naming: flat_<type>_<N>cols_<P>pct_<K>parts
#define FLUSH_FLAT_PARAM(type_name, kind, num_cols, null_pct, num_parts)      \
  BENCHMARK_NAMED_PARAM(                                                      \
      benchmarkFlushFlat,                                                     \
      type_name##_##num_cols##cols_##null_pct##pct_##num_parts##parts, \
      TypeKind::kind,                                                         \
      num_cols,                                                               \
      null_pct,                                                               \
      num_parts)

// Dimensions:
//   col type:       {bool, int, bigint, hugeint}
//   num cols:       {1, 4, 16, 64}
//   null constant:  {false, true}
//   num partitions: {1, 4, 16, 64, 256, 1024}
//
// Naming: constant_<type>_<N>cols_[non_]null_<K>parts
#define FLUSH_CONSTANT_PARAM(type_name, kind, num_cols, num_parts)           \
  BENCHMARK_NAMED_PARAM(                                                     \
      benchmarkFlushConstant,                                                \
      type_name##_##num_cols##cols_##notnull_##num_parts##parts,             \
      TypeKind::kind,                                                        \
      num_cols,                                                              \
      false,                                                                 \
      num_parts)

#define FLUSH_NULL_CONSTANT_PARAM(type_name, kind, num_cols, num_parts)  \
  BENCHMARK_NAMED_PARAM(                                                 \
      benchmarkFlushConstant,                                            \
      type_name##_##num_cols##cols_##null_##num_parts##parts,            \
      TypeKind::kind,                                                    \
      num_cols,                                                          \
      true,                                                              \
      num_parts)

#define FLUSH_FOR_NULLS(type_name, kind, num_cols, num_parts) \
  FLUSH_FLAT_PARAM(type_name, kind, num_cols, 0, num_parts)   \
  FLUSH_FLAT_PARAM(type_name, kind, num_cols, 25, num_parts)  \
  FLUSH_FLAT_PARAM(type_name, kind, num_cols, 50, num_parts)  \
  FLUSH_FLAT_PARAM(type_name, kind, num_cols, 75, num_parts)  \
  FLUSH_FLAT_PARAM(type_name, kind, num_cols, 100, num_parts) \
  FLUSH_CONSTANT_PARAM(type_name, kind, num_cols, num_parts)  \
  FLUSH_NULL_CONSTANT_PARAM(type_name, kind, num_cols, num_parts)

#define FLUSH_FOR_PARTS(type_name, kind, num_cols) \
  FLUSH_FOR_NULLS(type_name, kind, num_cols, 1)    \
  FLUSH_FOR_NULLS(type_name, kind, num_cols, 4)    \
  FLUSH_FOR_NULLS(type_name, kind, num_cols, 16)   \
  FLUSH_FOR_NULLS(type_name, kind, num_cols, 64)   \
  FLUSH_FOR_NULLS(type_name, kind, num_cols, 256)  \
  FLUSH_FOR_NULLS(type_name, kind, num_cols, 1024)

#define FLUSH_FOR_COLS(type_name, kind) \
  FLUSH_FOR_PARTS(type_name, kind, 1)   \
  FLUSH_FOR_PARTS(type_name, kind, 4)   \
  FLUSH_FOR_PARTS(type_name, kind, 16)  \
  FLUSH_FOR_PARTS(type_name, kind, 64)

FLUSH_FOR_COLS(bool, BOOLEAN)
FLUSH_FOR_COLS(int, INTEGER)
FLUSH_FOR_COLS(bigint, BIGINT)
FLUSH_FOR_COLS(ldec, HUGEINT)
FLUSH_FOR_COLS(varchar, VARCHAR)
FLUSH_FOR_COLS(varbinary, VARBINARY)

// Dictionary columns over a 1'024-value null-free base; nulls sit on the
// wrapper. Fixed-width kinds exercise the shared chunked value writer;
// VARCHAR exercises the variable-width dictionary path.
//
// Naming: dict_<type>_<N>cols_<P>pct_<K>parts
#define FLUSH_DICT_PARAM(type_name, kind, num_cols, null_pct, num_parts)     \
  BENCHMARK_NAMED_PARAM(                                                     \
      benchmarkFlushDictionary,                                              \
      dict_##type_name##_##num_cols##cols_##null_pct##pct_##num_parts##parts,\
      TypeKind::kind,                                                        \
      num_cols,                                                              \
      null_pct,                                                              \
      num_parts)

#define FLUSH_DICT_FOR_PARTS(type_name, kind, num_cols, null_pct) \
  FLUSH_DICT_PARAM(type_name, kind, num_cols, null_pct, 4)        \
  FLUSH_DICT_PARAM(type_name, kind, num_cols, null_pct, 64)       \
  FLUSH_DICT_PARAM(type_name, kind, num_cols, null_pct, 256)

FLUSH_DICT_FOR_PARTS(bigint, BIGINT, 1, 0)
FLUSH_DICT_FOR_PARTS(bigint, BIGINT, 1, 25)
FLUSH_DICT_FOR_PARTS(bigint, BIGINT, 1, 75)
FLUSH_DICT_FOR_PARTS(bigint, BIGINT, 4, 25)
FLUSH_DICT_FOR_PARTS(bool, BOOLEAN, 1, 25)
FLUSH_DICT_FOR_PARTS(varchar, VARCHAR, 1, 25)

// Nested ROW benchmarks: a single column with N levels of ROW wrapping a
// BIGINT leaf, across the same partition counts as the flat benchmarks.
//
// Naming: nestedrow_<N>levels_<K>parts
#define FLUSH_NESTED_ROW_PARAM(num_levels, num_parts)                  \
  BENCHMARK_NAMED_PARAM(                                               \
      benchmarkFlushNestedRow,                                         \
      nestedrow_##num_levels##levels_##num_parts##parts,               \
      num_levels,                                                      \
      num_parts)

#define FLUSH_NESTED_ROW_FOR_PARTS(num_levels)    \
  FLUSH_NESTED_ROW_PARAM(num_levels, 1)           \
  FLUSH_NESTED_ROW_PARAM(num_levels, 4)           \
  FLUSH_NESTED_ROW_PARAM(num_levels, 16)          \
  FLUSH_NESTED_ROW_PARAM(num_levels, 64)          \
  FLUSH_NESTED_ROW_PARAM(num_levels, 256)         \
  FLUSH_NESTED_ROW_PARAM(num_levels, 1024)

FLUSH_NESTED_ROW_FOR_PARTS(1)
FLUSH_NESTED_ROW_FOR_PARTS(2)
FLUSH_NESTED_ROW_FOR_PARTS(3)

// Nested ROW with nulls at every level. Naming:
// nestedrownulls_<N>levels_<P>pct_<K>parts
#define FLUSH_NESTED_ROW_NULLS_PARAM(num_levels, null_pct, num_parts)        \
  BENCHMARK_NAMED_PARAM(                                                     \
      benchmarkFlushNestedRowWithNulls,                                      \
      nestedrownulls_##num_levels##levels_##null_pct##pct_##num_parts##parts,\
      num_levels,                                                            \
      null_pct,                                                              \
      num_parts)

#define FLUSH_NESTED_ROW_NULLS_FOR_PARTS(num_levels, null_pct)         \
  FLUSH_NESTED_ROW_NULLS_PARAM(num_levels, null_pct, 1)                \
  FLUSH_NESTED_ROW_NULLS_PARAM(num_levels, null_pct, 4)                \
  FLUSH_NESTED_ROW_NULLS_PARAM(num_levels, null_pct, 16)               \
  FLUSH_NESTED_ROW_NULLS_PARAM(num_levels, null_pct, 64)               \
  FLUSH_NESTED_ROW_NULLS_PARAM(num_levels, null_pct, 256)              \
  FLUSH_NESTED_ROW_NULLS_PARAM(num_levels, null_pct, 1024)

FLUSH_NESTED_ROW_NULLS_FOR_PARTS(2, 25)
FLUSH_NESTED_ROW_NULLS_FOR_PARTS(2, 50)
FLUSH_NESTED_ROW_NULLS_FOR_PARTS(3, 25)
FLUSH_NESTED_ROW_NULLS_FOR_PARTS(3, 50)
// clang-format on

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  PrestoVectorSerde::registerVectorSerde();
  folly::runBenchmarks();
  return 0;
}
