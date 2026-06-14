/*
 * Copyright (c) International Business Machines
 * Corporation and others.  All Rights Reserved.
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
#include <iostream>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/BaseVector.h"

// Add the following definitions to allow Clion runs.
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

// Exercises Expr::evalWithMemo for CAST(BIGINT -> VARCHAR) and the fast
// numeric upcast CAST(INT -> BIGINT) over a stable dictionary base across
// many input vectors. This is the exact shape that drove the O(N^2)
// regression observed on a production worker - each input vector acquired
// one more string buffer into Expr::dictionaryCache_, so the per-call
// acquireSharedStringBuffers loop grew linearly with the number of vectors
// evaluated against the same base.
//
// READING THE BENCHMARK OUTPUT
//   Dict entries: BigintToVarchar(rowsPerVector_distinctValueCount_
//                                 newIndicesPerVector_nullPct)
//   Flat entries: BigintToVarchar(rowsPerVector_nullPct)
//
//   numVectors is fixed at kNumVectors (1000) for every entry and is
//   not encoded in the name. Example: `DICT_BigintToVarchar(100_5_1_0)`
//   means 100 rows per input vector, dictionary base has 5 distinct
//   values, each input vector introduces 1 new dictionary index
//   relative to the previous one, and 0% of dictionary positions are
//   null - over 1000 vectors per iteration.
//
// Parameter dimensions:
//   - numVectors          : how many input vectors per iteration
//                          (fixed at kNumVectors, not swept, not in name)
//   - rowsPerVector       : rows in each input vector
//   - distinctValueCount  : dictionary base cardinality (DICT only)
//   - newIndicesPerVector : new dictionary indices each input vector
//                           introduces (drives the cache miss rate)
//                           (DICT only)
//   - nullPct             : percentage of positions marked null. For
//                           DICT, nulls live on the dictionary wrap
//                           (reach PeeledEncoding::translateToInnerRows
//                           via wrapNulls_); for FLAT, nulls are on
//                           the flat input itself.
// Plus a flat (non-dictionary) baseline per rowsPerVector. The "from
// -> to" type pair appears in every entry name so the table is
// self-describing. The same legend is printed at startup before the
// benchmark output.
//
// A NOTE ON "0.00fs Infinity" ENTRIES
//   Folly normalises measurements by subtracting a globally-measured
//   baseline (the cost of an empty BENCHMARK loop, ~0.5 ns/iter) and
//   floors the result at zero (Benchmark.cpp:207). For configurations
//   where the dictionary cache covers the whole base in the first
//   couple of batches (small distinctValueCount relative to row count
//   per vector, e.g. dvc=5 with rowsPerVector=10000 - the bypass in
//   peelEncodings kicks in for the remaining ~998 batches), the
//   per-row cost falls below that baseline and folly displays 0.00fs.
//   It means "below resolution after baseline subtraction", not zero
//   actual work - the corresponding nullPct=100 row (which doesn't
//   hit the bypass because translateToInnerRows returns empty inner
//   rows and dictionaryCache_ never populates) still measures around
//   400-500 ps/iter, giving a sense of the floor folly can resolve.

using namespace facebook::velox;

namespace {

class WideningCastBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  WideningCastBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerAllScalarFunctions();
  }

  // Builds a flat numeric base of `baseType` and runs `numVectors`
  // evaluations of `expression` over dictionary wrappers of that base. Each
  // input vector's indices are `(row + vectorIdx * newIndicesPerVector) mod
  // distinctValueCount`, so consecutive vectors overlap by `rowsPerVector -
  // newIndicesPerVector` indices. `nullPct` is the percentage of dictionary
  // positions marked null on the wrap (0, 50, 100) - the wrap-level nulls
  // are what reach Expr::evalEncodings / PeeledEncoding::translateToInner-
  // Rows. nullPct=0 hits the hoisted no-nulls path; nullPct=50 exercises
  // the null-aware loop; nullPct=100 marks every position null so the
  // downstream cast work short-circuits.
  template <typename BaseNativeType>
  size_t runDictionary(
      const std::string& expression,
      const TypePtr& baseType,
      int32_t numVectors,
      int32_t rowsPerVector,
      int32_t distinctValueCount,
      int32_t newIndicesPerVector,
      int32_t nullPct) {
    folly::BenchmarkSuspender suspender;

    auto base = vectorMaker_.flatVector<BaseNativeType>(
        distinctValueCount,
        [](vector_size_t row) { return static_cast<BaseNativeType>(row + 1); },
        nullptr,
        baseType);
    auto rowType = ROW({"c0"}, {baseType});
    auto exprSet = compileExpression(expression, rowType);

    std::vector<RowVectorPtr> inputs;
    inputs.reserve(numVectors);
    for (int32_t vectorIdx = 0; vectorIdx < numVectors; ++vectorIdx) {
      const int32_t offset = vectorIdx * newIndicesPerVector;
      auto indices =
          AlignedBuffer::allocate<vector_size_t>(rowsPerVector, pool());
      auto* rawIndices = indices->asMutable<vector_size_t>();
      for (int32_t row = 0; row < rowsPerVector; ++row) {
        rawIndices[row] = (row + offset) % distinctValueCount;
      }

      BufferPtr nulls;
      if (nullPct > 0) {
        nulls = AlignedBuffer::allocate<bool>(rowsPerVector, pool());
        auto* rawNulls = nulls->asMutable<uint64_t>();
        if (nullPct >= 100) {
          bits::fillBits(rawNulls, 0, rowsPerVector, bits::kNull);
        } else {
          bits::fillBits(rawNulls, 0, rowsPerVector, bits::kNotNull);
          const int32_t step = 100 / nullPct;
          for (int32_t row = 0; row < rowsPerVector; row += step) {
            bits::setNull(rawNulls, row, true);
          }
        }
      }
      auto dict =
          BaseVector::wrapInDictionary(nulls, indices, rowsPerVector, base);
      inputs.push_back(vectorMaker_.rowVector({dict}));
    }
    suspender.dismiss();

    size_t count = 0;
    for (auto& input : inputs) {
      auto result = evaluate(exprSet, input);
      folly::doNotOptimizeAway(result);
      count += result->size();
    }
    return count;
  }

  template <typename BaseNativeType>
  size_t runFlat(
      const std::string& expression,
      const TypePtr& baseType,
      int32_t numVectors,
      int32_t rowsPerVector,
      int32_t nullPct) {
    folly::BenchmarkSuspender suspender;
    std::function<bool(vector_size_t)> isNullAt;
    if (nullPct > 0) {
      if (nullPct >= 100) {
        isNullAt = [](vector_size_t) { return true; };
      } else {
        const int32_t step = 100 / nullPct;
        isNullAt = [step](vector_size_t row) { return (row % step) == 0; };
      }
    }
    std::vector<RowVectorPtr> inputs;
    inputs.reserve(numVectors);
    for (int32_t vectorIdx = 0; vectorIdx < numVectors; ++vectorIdx) {
      auto flat = vectorMaker_.flatVector<BaseNativeType>(
          rowsPerVector,
          [vectorIdx](vector_size_t row) {
            return static_cast<BaseNativeType>(row + vectorIdx * 16 + 1);
          },
          isNullAt,
          baseType);
      inputs.push_back(vectorMaker_.rowVector({flat}));
    }
    auto exprSet = compileExpression(expression, inputs[0]->type());
    suspender.dismiss();

    size_t count = 0;
    for (auto& input : inputs) {
      count += evaluate(exprSet, input)->size();
    }
    return count;
  }
};

// Free functions used by BENCHMARK_NAMED_PARAM_MULTI. Names spell the
// from -> to type pair. Each loops the work `iters` times so folly's
// iteration-scaling controls measurement length.

unsigned DICT_BigintToVarchar(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionary<int64_t>(
        "cast(c0 as varchar)",
        BIGINT(),
        numVectors,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct);
  }
  return total;
}

unsigned FLAT_BigintToVarchar(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runFlat<int64_t>(
        "cast(c0 as varchar)", BIGINT(), numVectors, rowsPerVector, nullPct);
  }
  return total;
}

unsigned DICT_IntToBigint(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionary<int32_t>(
        "cast(c0 as bigint)",
        INTEGER(),
        numVectors,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct);
  }
  return total;
}

unsigned FLAT_IntToBigint(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runFlat<int32_t>(
        "cast(c0 as bigint)", INTEGER(), numVectors, rowsPerVector, nullPct);
  }
  return total;
}

// DATE is represented natively as int32 days-since-epoch.
unsigned DICT_DateToVarchar(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionary<int32_t>(
        "cast(c0 as varchar)",
        DATE(),
        numVectors,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct);
  }
  return total;
}

unsigned FLAT_DateToVarchar(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runFlat<int32_t>(
        "cast(c0 as varchar)", DATE(), numVectors, rowsPerVector, nullPct);
  }
  return total;
}

unsigned DICT_DateToTimestamp(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionary<int32_t>(
        "cast(c0 as timestamp)",
        DATE(),
        numVectors,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct);
  }
  return total;
}

unsigned FLAT_DateToTimestamp(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runFlat<int32_t>(
        "cast(c0 as timestamp)", DATE(), numVectors, rowsPerVector, nullPct);
  }
  return total;
}

unsigned DICT_RealToDouble(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionary<float>(
        "cast(c0 as double)",
        REAL(),
        numVectors,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct);
  }
  return total;
}

unsigned FLAT_RealToDouble(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runFlat<float>(
        "cast(c0 as double)", REAL(), numVectors, rowsPerVector, nullPct);
  }
  return total;
}

} // namespace

// DICT_BIGINT_TO_VARCHAR registers cast(BIGINT -> VARCHAR) over a
// dictionary for every (rowsPerVector, distinctValueCount,
// newIndicesPerVector) sweep point. numVectors is fixed at 1000 (the
// literal in the BENCHMARK_NAMED_PARAM_MULTI call below) and is not
// encoded in the entry name. Each source line through DICT_NULLS /
// FLAT_NULLS expands to three benchmark entries - one per nullPct in
// {0, 50, 100}. Comment a single line out of the body to drop those
// three combinations globally.
#define DICT(                                                                          \
    funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct)         \
  BENCHMARK_NAMED_PARAM_MULTI(                                                         \
      funcName,                                                                        \
      rowsPerVector##_##distinctValueCount##_##newIndicesPerVector##_##nullPct,        \
      1000,                                                                            \
      rowsPerVector,                                                                   \
      distinctValueCount,                                                              \
      newIndicesPerVector,                                                             \
      nullPct)

#define DICT_NULLS(                                                          \
    funcName, rowsPerVector, distinctValueCount, newIndicesPerVector)        \
  DICT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 0)  \
  DICT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 50) \
  DICT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 100)

#define FLAT(funcName, rowsPerVector, nullPct)         \
  BENCHMARK_NAMED_PARAM_MULTI(                         \
      funcName, rowsPerVector##_##nullPct, 1000, rowsPerVector, nullPct)

#define FLAT_NULLS(funcName, rowsPerVector) \
  FLAT(funcName, rowsPerVector, 0)          \
  FLAT(funcName, rowsPerVector, 50)         \
  FLAT(funcName, rowsPerVector, 100)

// Each DICT(funcName, ...) line below registers one benchmark entry.
// The three numeric arguments are positional:
//   DICT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector)
#define DICT_BIGINT_TO_VARCHAR                         \
  DICT_NULLS(DICT_BigintToVarchar, 100, 5, 1)      \
  DICT_NULLS(DICT_BigintToVarchar, 100, 5, 100)    \
  DICT_NULLS(DICT_BigintToVarchar, 100, 5, 1000)   \
  DICT_NULLS(DICT_BigintToVarchar, 100, 500, 1)    \
  DICT_NULLS(DICT_BigintToVarchar, 100, 500, 100)  \
  DICT_NULLS(DICT_BigintToVarchar, 100, 500, 1000) \
  DICT_NULLS(DICT_BigintToVarchar, 100, 15000, 1)  \
  DICT_NULLS(DICT_BigintToVarchar, 100, 15000, 100)\
  DICT_NULLS(DICT_BigintToVarchar, 100, 15000, 1000)\
  DICT_NULLS(DICT_BigintToVarchar, 1000, 5, 1)     \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 5, 100)   \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 5, 1000)  \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 500, 1)   \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 500, 100) \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 500, 1000)\
  DICT_NULLS(DICT_BigintToVarchar, 1000, 15000, 1) \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 15000, 100)\
  DICT_NULLS(DICT_BigintToVarchar, 1000, 15000, 1000)\
  DICT_NULLS(DICT_BigintToVarchar, 10000, 5, 1)    \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 5, 100)  \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 5, 1000) \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 500, 1)  \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 500, 100)\
  DICT_NULLS(DICT_BigintToVarchar, 10000, 500, 1000)\
  DICT_NULLS(DICT_BigintToVarchar, 10000, 15000, 1)\
  DICT_NULLS(DICT_BigintToVarchar, 10000, 15000, 100)\
  DICT_NULLS(DICT_BigintToVarchar, 10000, 15000, 1000)

#define FLAT_BIGINT_TO_VARCHAR                   \
  FLAT_NULLS(FLAT_BigintToVarchar, 100)      \
  FLAT_NULLS(FLAT_BigintToVarchar, 1000)     \
  FLAT_NULLS(FLAT_BigintToVarchar, 10000)

#define DICT_INT_TO_BIGINT                            \
  DICT_NULLS(DICT_IntToBigint, 100, 5, 1)         \
  DICT_NULLS(DICT_IntToBigint, 100, 5, 100)       \
  DICT_NULLS(DICT_IntToBigint, 100, 5, 1000)      \
  DICT_NULLS(DICT_IntToBigint, 100, 500, 1)       \
  DICT_NULLS(DICT_IntToBigint, 100, 500, 100)     \
  DICT_NULLS(DICT_IntToBigint, 100, 500, 1000)    \
  DICT_NULLS(DICT_IntToBigint, 100, 15000, 1)     \
  DICT_NULLS(DICT_IntToBigint, 100, 15000, 100)   \
  DICT_NULLS(DICT_IntToBigint, 100, 15000, 1000)  \
  DICT_NULLS(DICT_IntToBigint, 1000, 5, 1)        \
  DICT_NULLS(DICT_IntToBigint, 1000, 5, 100)      \
  DICT_NULLS(DICT_IntToBigint, 1000, 5, 1000)     \
  DICT_NULLS(DICT_IntToBigint, 1000, 500, 1)      \
  DICT_NULLS(DICT_IntToBigint, 1000, 500, 100)    \
  DICT_NULLS(DICT_IntToBigint, 1000, 500, 1000)   \
  DICT_NULLS(DICT_IntToBigint, 1000, 15000, 1)    \
  DICT_NULLS(DICT_IntToBigint, 1000, 15000, 100)  \
  DICT_NULLS(DICT_IntToBigint, 1000, 15000, 1000) \
  DICT_NULLS(DICT_IntToBigint, 10000, 5, 1)       \
  DICT_NULLS(DICT_IntToBigint, 10000, 5, 100)     \
  DICT_NULLS(DICT_IntToBigint, 10000, 5, 1000)    \
  DICT_NULLS(DICT_IntToBigint, 10000, 500, 1)     \
  DICT_NULLS(DICT_IntToBigint, 10000, 500, 100)   \
  DICT_NULLS(DICT_IntToBigint, 10000, 500, 1000)  \
  DICT_NULLS(DICT_IntToBigint, 10000, 15000, 1)   \
  DICT_NULLS(DICT_IntToBigint, 10000, 15000, 100) \
  DICT_NULLS(DICT_IntToBigint, 10000, 15000, 1000)

// Each FLAT(funcName, ...) line below registers one benchmark entry.
// The single numeric argument is:
//   FLAT(funcName, rowsPerVector)
#define FLAT_INT_TO_BIGINT                       \
  FLAT_NULLS(FLAT_IntToBigint, 100)          \
  FLAT_NULLS(FLAT_IntToBigint, 1000)         \
  FLAT_NULLS(FLAT_IntToBigint, 10000)

#define DICT_DATE_TO_VARCHAR                  \
  DICT_NULLS(DICT_DateToVarchar, 100, 5, 1)              \
  DICT_NULLS(DICT_DateToVarchar, 100, 5, 100)            \
  DICT_NULLS(DICT_DateToVarchar, 100, 5, 1000)           \
  DICT_NULLS(DICT_DateToVarchar, 100, 500, 1)            \
  DICT_NULLS(DICT_DateToVarchar, 100, 500, 100)          \
  DICT_NULLS(DICT_DateToVarchar, 100, 500, 1000)         \
  DICT_NULLS(DICT_DateToVarchar, 100, 15000, 1)          \
  DICT_NULLS(DICT_DateToVarchar, 100, 15000, 100)        \
  DICT_NULLS(DICT_DateToVarchar, 100, 15000, 1000)       \
  DICT_NULLS(DICT_DateToVarchar, 1000, 5, 1)             \
  DICT_NULLS(DICT_DateToVarchar, 1000, 5, 100)           \
  DICT_NULLS(DICT_DateToVarchar, 1000, 5, 1000)          \
  DICT_NULLS(DICT_DateToVarchar, 1000, 500, 1)           \
  DICT_NULLS(DICT_DateToVarchar, 1000, 500, 100)         \
  DICT_NULLS(DICT_DateToVarchar, 1000, 500, 1000)        \
  DICT_NULLS(DICT_DateToVarchar, 1000, 15000, 1)         \
  DICT_NULLS(DICT_DateToVarchar, 1000, 15000, 100)       \
  DICT_NULLS(DICT_DateToVarchar, 1000, 15000, 1000)      \
  DICT_NULLS(DICT_DateToVarchar, 10000, 5, 1)            \
  DICT_NULLS(DICT_DateToVarchar, 10000, 5, 100)          \
  DICT_NULLS(DICT_DateToVarchar, 10000, 5, 1000)         \
  DICT_NULLS(DICT_DateToVarchar, 10000, 500, 1)          \
  DICT_NULLS(DICT_DateToVarchar, 10000, 500, 100)        \
  DICT_NULLS(DICT_DateToVarchar, 10000, 500, 1000)       \
  DICT_NULLS(DICT_DateToVarchar, 10000, 15000, 1)        \
  DICT_NULLS(DICT_DateToVarchar, 10000, 15000, 100)      \
  DICT_NULLS(DICT_DateToVarchar, 10000, 15000, 1000)

#define FLAT_DATE_TO_VARCHAR    \
  FLAT_NULLS(FLAT_DateToVarchar, 100)      \
  FLAT_NULLS(FLAT_DateToVarchar, 1000)     \
  FLAT_NULLS(FLAT_DateToVarchar, 10000)

#define DICT_DATE_TO_TIMESTAMP                  \
  DICT_NULLS(DICT_DateToTimestamp, 100, 5, 1)              \
  DICT_NULLS(DICT_DateToTimestamp, 100, 5, 100)            \
  DICT_NULLS(DICT_DateToTimestamp, 100, 5, 1000)           \
  DICT_NULLS(DICT_DateToTimestamp, 100, 500, 1)            \
  DICT_NULLS(DICT_DateToTimestamp, 100, 500, 100)          \
  DICT_NULLS(DICT_DateToTimestamp, 100, 500, 1000)         \
  DICT_NULLS(DICT_DateToTimestamp, 100, 15000, 1)          \
  DICT_NULLS(DICT_DateToTimestamp, 100, 15000, 100)        \
  DICT_NULLS(DICT_DateToTimestamp, 100, 15000, 1000)       \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 5, 1)             \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 5, 100)           \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 5, 1000)          \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 500, 1)           \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 500, 100)         \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 500, 1000)        \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 15000, 1)         \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 15000, 100)       \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 15000, 1000)      \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 5, 1)            \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 5, 100)          \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 5, 1000)         \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 500, 1)          \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 500, 100)        \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 500, 1000)       \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 15000, 1)        \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 15000, 100)      \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 15000, 1000)

#define FLAT_DATE_TO_TIMESTAMP    \
  FLAT_NULLS(FLAT_DateToTimestamp, 100)      \
  FLAT_NULLS(FLAT_DateToTimestamp, 1000)     \
  FLAT_NULLS(FLAT_DateToTimestamp, 10000)

#define DICT_REAL_TO_DOUBLE                  \
  DICT_NULLS(DICT_RealToDouble, 100, 5, 1)              \
  DICT_NULLS(DICT_RealToDouble, 100, 5, 100)            \
  DICT_NULLS(DICT_RealToDouble, 100, 5, 1000)           \
  DICT_NULLS(DICT_RealToDouble, 100, 500, 1)            \
  DICT_NULLS(DICT_RealToDouble, 100, 500, 100)          \
  DICT_NULLS(DICT_RealToDouble, 100, 500, 1000)         \
  DICT_NULLS(DICT_RealToDouble, 100, 15000, 1)          \
  DICT_NULLS(DICT_RealToDouble, 100, 15000, 100)        \
  DICT_NULLS(DICT_RealToDouble, 100, 15000, 1000)       \
  DICT_NULLS(DICT_RealToDouble, 1000, 5, 1)             \
  DICT_NULLS(DICT_RealToDouble, 1000, 5, 100)           \
  DICT_NULLS(DICT_RealToDouble, 1000, 5, 1000)          \
  DICT_NULLS(DICT_RealToDouble, 1000, 500, 1)           \
  DICT_NULLS(DICT_RealToDouble, 1000, 500, 100)         \
  DICT_NULLS(DICT_RealToDouble, 1000, 500, 1000)        \
  DICT_NULLS(DICT_RealToDouble, 1000, 15000, 1)         \
  DICT_NULLS(DICT_RealToDouble, 1000, 15000, 100)       \
  DICT_NULLS(DICT_RealToDouble, 1000, 15000, 1000)      \
  DICT_NULLS(DICT_RealToDouble, 10000, 5, 1)            \
  DICT_NULLS(DICT_RealToDouble, 10000, 5, 100)          \
  DICT_NULLS(DICT_RealToDouble, 10000, 5, 1000)         \
  DICT_NULLS(DICT_RealToDouble, 10000, 500, 1)          \
  DICT_NULLS(DICT_RealToDouble, 10000, 500, 100)        \
  DICT_NULLS(DICT_RealToDouble, 10000, 500, 1000)       \
  DICT_NULLS(DICT_RealToDouble, 10000, 15000, 1)        \
  DICT_NULLS(DICT_RealToDouble, 10000, 15000, 100)      \
  DICT_NULLS(DICT_RealToDouble, 10000, 15000, 1000)

#define FLAT_REAL_TO_DOUBLE    \
  FLAT_NULLS(FLAT_RealToDouble, 100)      \
  FLAT_NULLS(FLAT_RealToDouble, 1000)     \
  FLAT_NULLS(FLAT_RealToDouble, 10000)

BENCHMARK_DRAW_LINE();
DICT_BIGINT_TO_VARCHAR
FLAT_BIGINT_TO_VARCHAR
BENCHMARK_DRAW_LINE();
DICT_INT_TO_BIGINT
FLAT_INT_TO_BIGINT
BENCHMARK_DRAW_LINE();
DICT_DATE_TO_VARCHAR
FLAT_DATE_TO_VARCHAR
BENCHMARK_DRAW_LINE();
DICT_DATE_TO_TIMESTAMP
FLAT_DATE_TO_TIMESTAMP
BENCHMARK_DRAW_LINE();
DICT_REAL_TO_DOUBLE
FLAT_REAL_TO_DOUBLE

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  std::cout
      << "\nBenchmark entry names encode the sweep parameters:\n"
      << "  DICT_<from>To<to>("
         "rowsPerVector_distinctValueCount_newIndicesPerVector_nullPct)\n"
      << "  FLAT_<from>To<to>(rowsPerVector_nullPct)\n"
      << "\n"
      << "numVectors is fixed at 1000 for every entry and is not encoded\n"
      << "in the name. Each measurement runs the cast over 1000 input\n"
      << "vectors back-to-back; the reported time/iter is amortized over\n"
      << "the total row count (1000 * rowsPerVector).\n"
      << "\n"
      << "  rowsPerVector       : rows in each input vector\n"
      << "  distinctValueCount  : dictionary base cardinality (DICT only)\n"
      << "  newIndicesPerVector : new dictionary indices each input vector\n"
      << "                        introduces vs the previous one - drives\n"
      << "                        the dictionary-cache miss rate (DICT only)\n"
      << "  nullPct             : percentage of positions marked null in\n"
      << "                        {0, 50, 100}. For DICT, nulls live on\n"
      << "                        the dictionary wrap; for FLAT, on the\n"
      << "                        flat input itself.\n"
      << "\n";

  folly::runBenchmarks();
  return 0;
}
