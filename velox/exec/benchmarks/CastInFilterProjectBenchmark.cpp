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

#include <iostream>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

/// Exercises CAST inside FilterProject over a dictionary-encoded input,
/// the shape that prompted FilterProject's filter/project result-vector
/// recycle path (FilterProject keeps filterResults_ / projectResults_ as
/// members, releases them to ExecCtx's VectorPool and clears any non-
/// poolable wrappers at the top of each call). The benchmark builds:
///
///   Values(dictionary-encoded input) -> Project(cast(c0 as <type>))
///
/// and sweeps rows-per-vector and dictionary cardinality to highlight
/// the per-call fixed cost that small vectors expose. With the recycle
/// path in place, the next batch's BaseVector::ensureWritable on the
/// cast result reuses the prior buffer in place instead of taking the
/// copy-on-write branch through MemoryPoolImpl::allocate.
///
/// READING THE OUTPUT
///   <CastPair>_<rowsPerVector>_<distinctValueCount>_<nullPct>
///
/// numVectors is fixed at kNumVectors (1000) and is not encoded in the
/// name. nullPct is the percentage of dictionary positions marked null
/// on the wrap. Each measurement runs the plan once over those 1000
/// vectors; folly normalises time by the row count returned, so the
/// printed time/iter is per row.

using namespace facebook::velox;
using namespace facebook::velox::exec;
using facebook::velox::test::VectorTestBase;

DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

namespace {

constexpr int32_t kNumVectors = 1000;

class CastInFilterProjectBenchmark : public VectorTestBase {
 public:
  // Builds `numVectors` row vectors, each `rowsPerVector` rows wide.
  // Every input vector wraps a flat base of `baseType` and
  // `distinctValueCount` distinct values in a DictionaryVector with
  // indices `(row + vectorIdx) % distinctValueCount` - so consecutive
  // vectors share the same underlying base (enabling
  // Expr::evalWithMemo's dictionaryCache_ to stay warm) but rotate
  // which positions are hit.
  // Builds `numVectors` dictionary-wrapped row vectors. nullPct is the
  // percentage of dictionary positions marked null on the wrap (not on
  // the underlying flat base). The wrap-level nulls are what reach
  // PeeledEncoding::translateToInnerRows via wrapNulls_ - 0 takes the
  // hoisted no-nulls fast path, 50 exercises the null-aware loop with
  // a mix, and 100 marks every row null so the inner-rows set comes
  // out empty.
  template <typename BaseNativeType>
  std::vector<RowVectorPtr> makeDictInput(
      const TypePtr& baseType,
      int32_t numVectors,
      int32_t rowsPerVector,
      int32_t distinctValueCount,
      int32_t nullPct) {
    auto base = makeFlatVector<BaseNativeType>(
        distinctValueCount,
        [](vector_size_t row) { return static_cast<BaseNativeType>(row + 1); },
        nullptr,
        baseType);

    std::vector<RowVectorPtr> vectors;
    vectors.reserve(numVectors);
    for (int32_t vectorIdx = 0; vectorIdx < numVectors; ++vectorIdx) {
      auto indices =
          AlignedBuffer::allocate<vector_size_t>(rowsPerVector, pool());
      auto* rawIndices = indices->asMutable<vector_size_t>();
      for (int32_t row = 0; row < rowsPerVector; ++row) {
        rawIndices[row] = (row + vectorIdx) % distinctValueCount;
      }

      BufferPtr nulls;
      if (nullPct > 0) {
        nulls = AlignedBuffer::allocate<bool>(rowsPerVector, pool());
        auto* rawNulls = nulls->asMutable<uint64_t>();
        if (nullPct >= 100) {
          // Every row null.
          bits::fillBits(rawNulls, 0, rowsPerVector, bits::kNull);
        } else {
          // Mark every (100/nullPct)-th row null; the rest are
          // non-null. For nullPct=50 that produces a regular
          // null/non-null/null/non-null pattern.
          bits::fillBits(rawNulls, 0, rowsPerVector, bits::kNotNull);
          const int32_t step = 100 / nullPct;
          for (int32_t row = 0; row < rowsPerVector; row += step) {
            bits::setNull(rawNulls, row, true);
          }
        }
      }
      auto dict =
          BaseVector::wrapInDictionary(nulls, indices, rowsPerVector, base);
      vectors.push_back(makeRowVector({dict}));
    }
    return vectors;
  }

  // Builds a Values -> Project plan applying `castExpr` to c0.
  std::shared_ptr<const core::PlanNode> makePlan(
      const std::vector<RowVectorPtr>& data,
      const std::string& castExpr) {
    exec::test::PlanBuilder builder;
    builder.values(data);
    builder.project({castExpr});
    return builder.planNode();
  }

  // Runs the plan once via AssertQueryBuilder; returns the total row
  // count produced.
  size_t run(const std::shared_ptr<const core::PlanNode>& plan) {
    auto result = exec::test::AssertQueryBuilder(plan).copyResults(pool());
    return result ? result->size() : 0;
  }
};

// Each free function below is one benchmark entry point. The returned
// value (total rows produced) tells folly the iteration count for
// per-row normalisation. Plans are rebuilt per iter (the underlying
// data vectors stay alive for the bench lifetime, but FilterProject's
// member state is fresh each Driver instantiation - that's the
// realistic worst case: a hot operator instance accumulates its
// recycle pool over many getOutput() calls, whereas a cold one starts
// empty).

template <typename BaseNativeType>
size_t runDict(
    unsigned iters,
    const TypePtr& baseType,
    const std::string& castExpr,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t nullPct) {
  CastInFilterProjectBenchmark bm;
  auto data = bm.makeDictInput<BaseNativeType>(
      baseType, kNumVectors, rowsPerVector, distinctValueCount, nullPct);
  auto plan = bm.makePlan(data, castExpr);

  size_t total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += bm.run(plan);
  }
  return total;
}

unsigned DICT_IntToBigint(
    unsigned iters,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t nullPct) {
  return runDict<int32_t>(
      iters,
      INTEGER(),
      "cast(c0 as bigint)",
      rowsPerVector,
      distinctValueCount,
      nullPct);
}

unsigned DICT_RealToDouble(
    unsigned iters,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t nullPct) {
  return runDict<float>(
      iters,
      REAL(),
      "cast(c0 as double)",
      rowsPerVector,
      distinctValueCount,
      nullPct);
}

unsigned DICT_BigintToVarchar(
    unsigned iters,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t nullPct) {
  return runDict<int64_t>(
      iters,
      BIGINT(),
      "cast(c0 as varchar)",
      rowsPerVector,
      distinctValueCount,
      nullPct);
}

} // namespace

#define DICT(funcName, rowsPerVector, distinctValueCount, nullPct)      \
  BENCHMARK_NAMED_PARAM_MULTI(                                          \
      funcName,                                                         \
      rowsPerVector##_##distinctValueCount##_##nullPct,                 \
      rowsPerVector,                                                    \
      distinctValueCount,                                               \
      nullPct)

#define SWEEP_AT(funcName, rowsPerVector, distinctValueCount) \
  DICT(funcName, rowsPerVector, distinctValueCount, 0)        \
  DICT(funcName, rowsPerVector, distinctValueCount, 50)       \
  DICT(funcName, rowsPerVector, distinctValueCount, 100)

#define SWEEP(funcName)                  \
  SWEEP_AT(funcName, 1000, 5)            \
  SWEEP_AT(funcName, 1000, 500)          \
  SWEEP_AT(funcName, 1000, 15000)        \
  SWEEP_AT(funcName, 10000, 5)           \
  SWEEP_AT(funcName, 10000, 500)         \
  SWEEP_AT(funcName, 10000, 15000)

BENCHMARK_DRAW_LINE();
SWEEP(DICT_IntToBigint)
// BENCHMARK_DRAW_LINE();
// SWEEP(DICT_RealToDouble)
// BENCHMARK_DRAW_LINE();
// SWEEP(DICT_BigintToVarchar)

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  std::cout
      << "\nBenchmark entry names encode the sweep parameters:\n"
      << "  DICT_<from>To<to>(rowsPerVector_distinctValueCount_nullPct)\n"
      << "\n"
      << "numVectors is fixed at " << kNumVectors
      << " for every entry and is not encoded in the name. Each measurement\n"
      << "runs a Values -> Project plan over those " << kNumVectors
      << " dictionary-encoded vectors; folly normalises time by the row count\n"
      << "produced, so the printed time/iter is per row.\n"
      << "\n"
      << "  rowsPerVector       : rows in each input vector\n"
      << "  distinctValueCount  : dictionary base cardinality. All vectors\n"
      << "                        share the same flat base of this size,\n"
      << "                        so the second-and-subsequent vectors hit\n"
      << "                        Expr::evalWithMemo's cache path.\n"
      << "  nullPct             : percentage of dictionary positions marked\n"
      << "                        null on the wrap. 0 takes the hoisted no-\n"
      << "                        nulls path in translateToInnerRows; 50\n"
      << "                        exercises the null-aware loop; 100 marks\n"
      << "                        every position null.\n"
      << "\n";

  folly::runBenchmarks();
  return 0;
}
