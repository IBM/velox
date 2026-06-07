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
#include <algorithm>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/vector/PartitionedVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

// Add the following definitions to allow Clion runs
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");
DEFINE_uint32(
    virtual_partition_target,
    256,
    "Target number of virtual partitions. Set to 0 to disable.");

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::velox::test {

namespace {

thread_local auto gen = std::mt19937(42);

const std::function<bool(vector_size_t)> noNulls;

auto allNulls = [](vector_size_t) { return true; };

auto halfNulls = [](vector_size_t row) { return row % 2 == 0; };

template <TypeKind T>
RowTypePtr scalarTypeGenerator(int32_t numColumns) {
  return ROW(std::vector<TypePtr>(numColumns, createScalarType<T>()));
}

RowTypePtr dateTypeGenerator(int32_t numColumns) {
  return ROW(std::vector<TypePtr>(numColumns, DATE()));
}

RowTypePtr shortDecimalTypeGenerator(int32_t numColumns) {
  return ROW(std::vector<TypePtr>(numColumns, DECIMAL(10, 2)));
}

RowTypePtr longDecimalTypeGenerator(int32_t numColumns) {
  return ROW(std::vector<TypePtr>(numColumns, DECIMAL(20, 3)));
}

RowTypePtr mixedFlatTypeGenerator(int32_t numColumns) {
  const std::vector<TypePtr> typeSelection = {
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      HUGEINT(),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      DATE(),
      DECIMAL(10, 2),
      DECIMAL(20, 3),
  };

  std::vector<TypePtr> types;
  types.reserve(numColumns);

  for (int i = 0; i < numColumns; ++i) {
    types.push_back(typeSelection[i % typeSelection.size()]);
  }

  std::ranges::shuffle(types, gen);

  return ROW(std::move(types));
}

auto randomPartitionFunction = [](const RowVectorPtr& vector,
                                  uint32_t numPartitions,
                                  std::vector<uint32_t>& partitions) {
  partitions.resize(vector->size());
  for (int i = 0; i < vector->size(); ++i) {
    partitions[i] = gen() % numPartitions;
  }
};

/// Builds benchmark row vectors, one column at a time.
class VectorBuilder : public VectorTestBase {
 public:
  RowVectorPtr makeRowVector(
      const RowTypePtr& rowType,
      vector_size_t numRows,
      const std::function<bool(vector_size_t)>& isNullAt) {
    std::vector<VectorPtr> children;
    children.reserve(rowType->size());
    for (auto i = 0; i < rowType->size(); ++i) {
      children.push_back(makeColumn(rowType->childAt(i), numRows, isNullAt));
    }
    return VectorTestBase::makeRowVector(children);
  }

 private:
  VectorPtr makeColumn(
      const TypePtr& type,
      vector_size_t size,
      const std::function<bool(vector_size_t)>& isNullAt) {
    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return makeFlatVector<bool>(
            size, [](auto row) { return row % 2 == 0; }, isNullAt, type);
      case TypeKind::TINYINT:
        return makeFlatVector<int8_t>(
            size,
            [](auto row) { return static_cast<int8_t>(row); },
            isNullAt,
            type);
      case TypeKind::SMALLINT:
        return makeFlatVector<int16_t>(
            size,
            [](auto row) { return static_cast<int16_t>(row); },
            isNullAt,
            type);
      case TypeKind::INTEGER:
        if (type->isDate()) {
          return makeFlatVector<int32_t>(
              size,
              [](auto row) { return static_cast<int32_t>(row); },
              isNullAt,
              type);
        }
        return makeFlatVector<int32_t>(
            size, [](auto row) { return row; }, isNullAt, type);
      case TypeKind::BIGINT:
        return makeFlatVector<int64_t>(
            size,
            [](auto row) { return static_cast<int64_t>(row); },
            isNullAt,
            type);
      case TypeKind::HUGEINT:
        return makeFlatVector<int128_t>(
            size,
            [](auto row) { return static_cast<int128_t>(row); },
            isNullAt,
            type);
      case TypeKind::REAL:
        return makeFlatVector<float>(
            size,
            [](auto row) { return static_cast<float>(row); },
            isNullAt,
            type);
      case TypeKind::DOUBLE:
        return makeFlatVector<double>(
            size,
            [](auto row) { return static_cast<double>(row); },
            isNullAt,
            type);
      case TypeKind::TIMESTAMP:
        return makeFlatVector<Timestamp>(
            size,
            [](auto row) { return Timestamp(row, row * 1'000); },
            isNullAt,
            type);
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        // Alternate between short inlined strings (≤12 bytes) and long
        // out-of-line strings (>12 bytes) to exercise both StringView paths.
        return makeFlatVector<std::string>(
            size,
            [](auto row) -> std::string {
              if (row % 2 == 0) {
                return fmt::format("v-{}", row);
              }
              return fmt::format("velox_benchmark_string_{:08d}", row);
            },
            isNullAt,
            type);
      default:
        VELOX_UNSUPPORTED("Unsupported benchmark type: {}", type->toString());
    }
  }
};

} // namespace

// Picks the largest power-of-two fanout with numPartitions * fanout <= target.
// Returns 1 to disable virtual partitioning (when target is 0 or the logical
// partition count already meets the target). Mirrors what
// OptimizedPartitionedOutput would pick from its kVirtualPartitionTarget.
uint32_t pickFanout(uint32_t numPartitions, uint32_t target) {
  if (numPartitions < 2 || target <= numPartitions) {
    return 1;
  }
  uint32_t fanout = 1;
  while (numPartitions * fanout * 2 <= target) {
    fanout *= 2;
  }
  return fanout;
}

/// benchmark entry; construction is outside the timed region.
void runBM(
    uint32_t iterations,
    const std::function<RowTypePtr(int32_t)>& rowTypeGenerator,
    int32_t numColumns,
    uint32_t numPartitions,
    const std::function<bool(vector_size_t)>& isNullAt = noNulls,
    vector_size_t numRows = 10'000) {
  folly::BenchmarkSuspender suspender;
  VectorBuilder vectorBuilder;
  auto pool = memory::memoryManager()->addLeafPool();

  // Simulate what OptimizedPartitionedOutput does: pick a fanout, ask the
  // partition function to emit ids in the expanded virtual id space, then
  // pass that count via ctx.numVirtualPartitions. No striping happens here.
  const uint32_t fanout =
      pickFanout(numPartitions, FLAGS_virtual_partition_target);
  const uint32_t numVirtualPartitions = numPartitions * fanout;

  PartitionBuildContext ctx;
  ctx.numVirtualPartitions = numVirtualPartitions;

  auto vector = vectorBuilder.makeRowVector(
      rowTypeGenerator(numColumns), numRows, isNullAt);
  std::vector<uint32_t> partitions;
  randomPartitionFunction(vector, numVirtualPartitions, partitions);

  for (uint32_t i = 0; i < iterations; ++i) {
    const auto vectorCopy = std::static_pointer_cast<RowVector>(
        BaseVector::copy(*vector, pool.get()));
    suspender.dismiss();
    PartitionedVector::create(
        vectorCopy, partitions, numPartitions, ctx, pool.get());
    suspender.rehire();
  }
}

#define BENCHMARK_CONFIG(name, generator, numCols, nulls, numParts) \
  BENCHMARK_NAMED_PARAM(                                            \
      runBM,                                                        \
      name##_##numCols##Cols_##nulls##_P##numParts,                 \
      generator,                                                    \
      numCols,                                                      \
      numParts,                                                     \
      nulls);

#define BENCHMARK_PARTITIONS(name, generator, numCols, nulls) \
  BENCHMARK_CONFIG(name, generator, numCols, nulls, 4)        \
  BENCHMARK_CONFIG(name, generator, numCols, nulls, 16)       \
  BENCHMARK_CONFIG(name, generator, numCols, nulls, 64)       \
  BENCHMARK_CONFIG(name, generator, numCols, nulls, 256)      \
  BENCHMARK_CONFIG(name, generator, numCols, nulls, 1024)

#define BENCHMARK_SIZES(name, generator, nulls)     \
  BENCHMARK_PARTITIONS(name, generator, 1, nulls)   \
  BENCHMARK_PARTITIONS(name, generator, 10, nulls)  \
  BENCHMARK_PARTITIONS(name, generator, 100, nulls) \
  BENCHMARK_PARTITIONS(name, generator, 1000, nulls)

#define BENCHMARK_TYPE(name, generator)      \
  BENCHMARK_SIZES(name, generator, noNulls)  \
  BENCHMARK_SIZES(name, generator, allNulls) \
  BENCHMARK_SIZES(name, generator, halfNulls)

BENCHMARK_TYPE(BOOLEAN, scalarTypeGenerator<TypeKind::BOOLEAN>);
BENCHMARK_TYPE(SMALLINT, scalarTypeGenerator<TypeKind::SMALLINT>);
BENCHMARK_TYPE(INTEGER, scalarTypeGenerator<TypeKind::INTEGER>);
BENCHMARK_TYPE(BIGINT, scalarTypeGenerator<TypeKind::BIGINT>);
BENCHMARK_TYPE(HUGEINT, scalarTypeGenerator<TypeKind::HUGEINT>);
BENCHMARK_TYPE(REAL, scalarTypeGenerator<TypeKind::REAL>);
BENCHMARK_TYPE(DOUBLE, scalarTypeGenerator<TypeKind::DOUBLE>);
BENCHMARK_TYPE(TIMESTAMP, scalarTypeGenerator<TypeKind::TIMESTAMP>);
BENCHMARK_TYPE(VARCHAR, scalarTypeGenerator<TypeKind::VARCHAR>);
BENCHMARK_TYPE(VARBINARY, scalarTypeGenerator<TypeKind::VARBINARY>);
BENCHMARK_TYPE(DATE, dateTypeGenerator);
BENCHMARK_TYPE(ShortDecimal, shortDecimalTypeGenerator);
BENCHMARK_TYPE(LongDecimal, longDecimalTypeGenerator);
BENCHMARK_TYPE(Mixed, mixedFlatTypeGenerator);

} // namespace facebook::velox::test

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
