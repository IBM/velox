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

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Nulls.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::test {

// ctor Tests

void BM_ctor(uint32_t iterations, size_t numEntries) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::doNotOptimizeAway(SelectivityVector(numEntries));
  }
}

BENCHMARK_PARAM(BM_ctor, 1);
BENCHMARK_PARAM(BM_ctor, 1000);
BENCHMARK_PARAM(BM_ctor, 1000000);
BENCHMARK_PARAM(BM_ctor, 10000000);
BENCHMARK_DRAW_LINE();

// setValid Tests

template <bool Valid>
void setValidTest(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    // If we don't do this mod and instead hardcode, the compiler is going to
    // optimize while inlineing and skip the branching
    vector.setValid(i % numEntries, Valid);
  }

  folly::doNotOptimizeAway(vector);
  suspender.rehire();
}

void BM_setValidDynamic(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    // The i % 2 thing is to prevent the compiler doing magic with the constant
    // and having std::vector look ridiculously fast.
    vector.setValid(i % numEntries, i % 2 == 0);
  }

  folly::doNotOptimizeAway(vector);
  suspender.rehire();
}

void BM_setValidTrue(uint32_t iterations, size_t numEntries) {
  setValidTest<true>(iterations, numEntries);
}

void BM_setValidFalse(uint32_t iterations, size_t numEntries) {
  setValidTest<false>(iterations, numEntries);
}

BENCHMARK_PARAM(BM_setValidTrue, 1);
BENCHMARK_PARAM(BM_setValidTrue, 1000);
BENCHMARK_PARAM(BM_setValidTrue, 1000000);
BENCHMARK_PARAM(BM_setValidTrue, 10000000);
BENCHMARK_DRAW_LINE();

BENCHMARK_PARAM(BM_setValidFalse, 1);
BENCHMARK_PARAM(BM_setValidFalse, 1000);
BENCHMARK_PARAM(BM_setValidFalse, 1000000);
BENCHMARK_PARAM(BM_setValidFalse, 10000000);
BENCHMARK_DRAW_LINE();

BENCHMARK_PARAM(BM_setValidDynamic, 1);
BENCHMARK_PARAM(BM_setValidDynamic, 1000);
BENCHMARK_PARAM(BM_setValidDynamic, 1000000);
BENCHMARK_PARAM(BM_setValidDynamic, 10000000);
BENCHMARK_DRAW_LINE();

// setValidRange Tests

void setValidRangeTest(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  const size_t startIndexToSetIncl = numEntries / 10;
  const size_t endIndexToSetExcl = numEntries - startIndexToSetIncl;
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    vector.setValidRange(startIndexToSetIncl, endIndexToSetExcl, false);
  }
  suspender.rehire();
}

void BM_setValidRange(uint32_t iterations, size_t numEntries) {
  setValidRangeTest(iterations, numEntries);
}

BENCHMARK_PARAM(BM_setValidRange, 1);
BENCHMARK_PARAM(BM_setValidRange, 1000);
BENCHMARK_PARAM(BM_setValidRange, 1000000);
BENCHMARK_PARAM(BM_setValidRange, 10000000);
BENCHMARK_DRAW_LINE();

// isValid Tests

void BM_isValid(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  // The vector being constant also seems to influence the compiler, so changing
  // some bits
  for (size_t i = 0; i < vector.size(); ++i) {
    vector.setValid(i, folly::Random::oneIn(2));
  }
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    // Note: Without the % numEntries, the compiler seems to optimize away the
    // check. Unfortunately mod in itself also adds its own overhead.
    folly::doNotOptimizeAway(vector.isValid(i % numEntries));
    // folly:: doNotOptimizeAway(vector.isValid(indexToGet));
  }

  folly::doNotOptimizeAway(vector);
  suspender.rehire();
}

BENCHMARK_PARAM(BM_isValid, 1);
BENCHMARK_PARAM(BM_isValid, 1000);
BENCHMARK_PARAM(BM_isValid, 1000000);
BENCHMARK_PARAM(BM_isValid, 10000000);
BENCHMARK_DRAW_LINE();

// clearAll Tests

void BM_clearAll(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    vector.clearAll();
  }

  suspender.rehire();
}

BENCHMARK_PARAM(BM_clearAll, 1);
BENCHMARK_PARAM(BM_clearAll, 1000);
BENCHMARK_PARAM(BM_clearAll, 1000000);
BENCHMARK_PARAM(BM_clearAll, 10000000);
BENCHMARK_DRAW_LINE();

// setAll Tests

void BM_setAll(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    vector.setAll();
  }

  suspender.rehire();
}

BENCHMARK_PARAM(BM_setAll, 1);
BENCHMARK_PARAM(BM_setAll, 1000);
BENCHMARK_PARAM(BM_setAll, 1000000);
BENCHMARK_PARAM(BM_setAll, 10000000);
BENCHMARK_DRAW_LINE();

// merge Tests

void BM_merge(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vectorA(numEntries);
  SelectivityVector vectorB(numEntries);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    vectorA.intersect(vectorB);
  }

  suspender.rehire();
}

BENCHMARK_PARAM(BM_merge, 1);
BENCHMARK_PARAM(BM_merge, 1000);
BENCHMARK_PARAM(BM_merge, 1000000);
BENCHMARK_PARAM(BM_merge, 10000000);
BENCHMARK_DRAW_LINE();

// deselect Tests

void BM_deselect(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vectorA(numEntries);
  SelectivityVector vectorB(numEntries);
  // By clearing, the AND NOT of deselect will give all 1s
  vectorB.clearAll();
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    vectorA.deselect(vectorB);
  }

  suspender.rehire();
}

BENCHMARK_PARAM(BM_deselect, 1);
BENCHMARK_PARAM(BM_deselect, 1000);
BENCHMARK_PARAM(BM_deselect, 1000000);
BENCHMARK_PARAM(BM_deselect, 10000000);
BENCHMARK_DRAW_LINE();

// updateBounds Tests

void BM_updateBounds(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  vector.clearAll();
  // Set the middle valid so that finding the end isn't short-circuited
  vector.setValid(numEntries / 2, true);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    vector.updateBounds();
  }

  suspender.rehire();
}

BENCHMARK_PARAM(BM_updateBounds, 1);
BENCHMARK_PARAM(BM_updateBounds, 1000);
BENCHMARK_PARAM(BM_updateBounds, 1000000);
BENCHMARK_PARAM(BM_updateBounds, 10000000);
BENCHMARK_DRAW_LINE();

// countSelected Tests

void BM_countSelected(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vector(numEntries);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    folly::doNotOptimizeAway(vector.countSelected());
  }

  suspender.rehire();
}

BENCHMARK_PARAM(BM_countSelected, 1);
BENCHMARK_PARAM(BM_countSelected, 1000);
BENCHMARK_PARAM(BM_countSelected, 1000000);
BENCHMARK_PARAM(BM_countSelected, 10000000);
BENCHMARK_DRAW_LINE();

// translateToInnerRows Tests
//
// Measures the free function velox::translateToInnerRows used by
// PeeledEncoding::translateToInnerRows when an Expr peels a dictionary
// wrapping. It walks every selected outer row, optionally checks
// whether the corresponding dictionary position is null, and sets the
// inner-row bit at indices[row] when not null. The hoisted-branch fast
// path (nulls == nullptr) is the common production case; nullPct=50
// exercises the null-aware loop; nullPct=100 marks every position
// null so no inner row is selected.

void BM_translateToInnerRows(
    uint32_t iterations,
    size_t numEntries,
    int32_t nullPct) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector outerRows(numEntries);
  SelectivityVector innerRows(numEntries);
  innerRows.clearAll();

  std::vector<vector_size_t> indices(numEntries);
  for (size_t i = 0; i < numEntries; ++i) {
    // Identity mapping: indices[row] == row.
    indices[i] = static_cast<vector_size_t>(i);
  }

  std::vector<uint64_t> nullsStorage;
  const uint64_t* nulls = nullptr;
  if (nullPct > 0) {
    nullsStorage.resize((numEntries + 63) / 64);
    if (nullPct >= 100) {
      bits::fillBits(nullsStorage.data(), 0, numEntries, bits::kNull);
    } else {
      bits::fillBits(nullsStorage.data(), 0, numEntries, bits::kNotNull);
      const int32_t step = 100 / nullPct;
      for (size_t i = 0; i < numEntries; i += step) {
        bits::setNull(nullsStorage.data(), i, true);
      }
    }
    nulls = nullsStorage.data();
  }
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    translateToInnerRows(outerRows, indices.data(), nulls, innerRows);
    folly::doNotOptimizeAway(innerRows);
  }
  suspender.rehire();
}

BENCHMARK_NAMED_PARAM(BM_translateToInnerRows, 1000_0, 1000, 0);
BENCHMARK_NAMED_PARAM(BM_translateToInnerRows, 1000_50, 1000, 50);
BENCHMARK_NAMED_PARAM(BM_translateToInnerRows, 1000_100, 1000, 100);
BENCHMARK_NAMED_PARAM(BM_translateToInnerRows, 1000000_0, 1000000, 0);
BENCHMARK_NAMED_PARAM(BM_translateToInnerRows, 1000000_50, 1000000, 50);
BENCHMARK_NAMED_PARAM(BM_translateToInnerRows, 1000000_100, 1000000, 100);
BENCHMARK_DRAW_LINE();

// operatorEquals test

void BM_operatorEquals(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  SelectivityVector vectorA(numEntries);
  SelectivityVector vectorB(numEntries);
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    folly::doNotOptimizeAway(vectorA == vectorB);
  }

  suspender.rehire();
}

BENCHMARK_PARAM(BM_operatorEquals, 1);
BENCHMARK_PARAM(BM_operatorEquals, 1000);
BENCHMARK_PARAM(BM_operatorEquals, 1000000);
BENCHMARK_PARAM(BM_operatorEquals, 10000000);
BENCHMARK_DRAW_LINE();

} // namespace facebook::velox::test

// To run:
// buck run @mode/opt-clang-thinlto \
//   velox/vector/benchmarks:selectivity_vector_benchmark
int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  folly::runBenchmarks();
  return 0;
}
