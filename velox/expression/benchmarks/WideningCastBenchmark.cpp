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

#include <atomic>
#include <iostream>
#include <thread>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/BaseVector.h"

// Add the following definitions to allow Clion runs.
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

// Exercises Expr::evalWithMemo for CAST(BIGINT -> VARCHAR), the fast
// numeric upcast CAST(INT -> BIGINT), CAST(DATE -> VARCHAR/TIMESTAMP),
// CAST(REAL -> DOUBLE), and a production-shaped
// `date_format(CAST(date_trunc(...)) AS timestamp), '%Y-%m-%d')` chain
// over many input vectors. This is the exact shape that drove the
// O(N^2) regression observed on a production worker - each input
// vector acquired one more string buffer into Expr::dictionaryCache_,
// so the per-call acquireSharedStringBuffers loop grew linearly with
// the number of vectors evaluated against the same base.
//
// READING THE BENCHMARK OUTPUT
//   Single-base dict entries (no rotation):
//     <funcName>(rowsPerVector_distinctValueCount_
//                newIndicesPerVector_nullPct)
//   Multi-base dict entries (rotation):
//     <funcName>(rowsPerVector_distinctValueCount_
//                newIndicesPerVector_nullPct_bpb<batchesPerBase>)
//   Multi-thread dict entries:
//     MT_<funcName>(threads<n>_rowsPerVector_distinctValueCount_
//                   newIndicesPerVector_nullPct_bpb<batchesPerBase>)
//   Flat entries: <funcName>(rowsPerVector_nullPct)
//
//   numVectors is fixed at kNumVectors (1000) for every entry (per
//   worker thread for MT entries) and is not encoded in the name.
//   Example: `DICT_BigintToVarchar(100_5_1_0)` means 100 rows per
//   input vector, dictionary base has 5 distinct values, each input
//   vector introduces 1 new dictionary index relative to the
//   previous one, and 0% of dictionary positions are null - over
//   1000 vectors per iteration.
//
// Parameter dimensions:
//   - numVectors          : how many input vectors per iteration
//                          (fixed at 1000 per worker thread,
//                          not swept, not in name)
//   - rowsPerVector       : rows in each input vector
//   - distinctValueCount  : dictionary base cardinality (DICT only)
//   - newIndicesPerVector : new dictionary indices each input vector
//                           introduces (drives the cache miss rate
//                           against a stable base) (DICT only)
//   - nullPct             : percentage of positions marked null. For
//                           DICT, nulls live on the dictionary wrap
//                           (reach PeeledEncoding::translateToInnerRows
//                           via wrapNulls_); for FLAT, nulls are on
//                           the flat input itself.
//   - batchesPerBase      : how many consecutive batches reuse the
//                           same base FlatVector before rotating to
//                           the next one. The "single base" entries
//                           use batchesPerBase = 1000 (= numVectors,
//                           so the base never rotates). The "_bpb*"
//                           sweep covers {1, 10, 100} - matching the
//                           realistic mix of base-change events that
//                           a scan operator produces as it advances
//                           through splits / pages. Lower values mean
//                           more numMemoBaseChange events per call;
//                           higher values mean a steadier cache.
//   - numThreads          : MT entries only. Number of worker threads
//                           all evaluating against shared base
//                           FlatVector(s) - reproduces the cross-
//                           driver atomic refcount contention on the
//                           source Buffer that was observed in
//                           production VARCHAR-producing casts.
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

// Production expression from a slow query observed in prod:
// `date_format(CAST(date_trunc('day', date_add('day', 0 - (((day_of_week
//  (c0) % 7) - 1 + 7) % 7), c0)) AS timestamp), '%Y-%m-%d')`
// Hot-cache reproducer for the VARCHAR refcount-on-stringBuffer cost.
constexpr const char* kDateFormatProdExpr =
    "date_format("
    "cast(date_trunc('day', "
    "date_add('day', 0 - mod((day_of_week(c0) % 7 - 1) + 7, 7), c0)) "
    "as timestamp), '%Y-%m-%d')";

class WideningCastBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  WideningCastBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerAllScalarFunctions();
  }

  // Builds a flat numeric base of `baseType` and runs `numVectors`
  // evaluations of `expression` over dictionary wrappers of that
  // base. Each input vector's indices are `(row + vectorIdx *
  // newIndicesPerVector) mod distinctValueCount`, so consecutive
  // vectors overlap by `rowsPerVector - newIndicesPerVector`
  // indices. `nullPct` is the percentage of dictionary positions
  // marked null on the wrap (0, 50, 100). `batchesPerBase` controls
  // how often the underlying base FlatVector is swapped for a new
  // one: every `batchesPerBase` consecutive batches share a base,
  // then the next group of batches gets a different base (different
  // BufferPtr, different content). Pass `batchesPerBase >=
  // numVectors` (or <= 0) for the single-base behavior.
  template <typename BaseNativeType>
  size_t runDictionary(
      const std::string& expression,
      const TypePtr& baseType,
      int32_t numVectors,
      int32_t rowsPerVector,
      int32_t distinctValueCount,
      int32_t newIndicesPerVector,
      int32_t nullPct,
      int32_t batchesPerBase) {
    folly::BenchmarkSuspender suspender;
    if (batchesPerBase <= 0) {
      batchesPerBase = numVectors;
    }

    auto bases = buildBases<BaseNativeType>(
        baseType, distinctValueCount, numVectors, batchesPerBase, pool());
    auto rowType = ROW({"c0"}, {baseType});
    auto exprSet = compileExpression(expression, rowType);

    std::vector<RowVectorPtr> inputs;
    inputs.reserve(numVectors);
    for (int32_t vectorIdx = 0; vectorIdx < numVectors; ++vectorIdx) {
      const int32_t offset = vectorIdx * newIndicesPerVector;
      const int32_t baseIdx = vectorIdx / batchesPerBase;
      auto dict = wrapInDict(
          bases[baseIdx],
          offset,
          rowsPerVector,
          distinctValueCount,
          nullPct,
          pool());
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

  // Same as runDictionary but spawns `numThreads` worker threads.
  // The base FlatVectors are constructed once on the calling thread
  // and shared (via shared_ptr / BufferPtr) across all workers - this
  // puts the source Buffer's atomic refcount on a cache line touched
  // by every worker, reproducing the cross-driver contention observed
  // in production. Each worker has its own ExecCtx, ExprSet, and per-
  // thread input vectors (constructed on a per-thread pool); only the
  // base FlatVectors are shared.
  template <typename BaseNativeType>
  size_t runDictionaryMultiThread(
      const std::string& expression,
      const TypePtr& baseType,
      int32_t numThreads,
      int32_t numVectorsPerThread,
      int32_t rowsPerVector,
      int32_t distinctValueCount,
      int32_t newIndicesPerVector,
      int32_t nullPct,
      int32_t batchesPerBase) {
    folly::BenchmarkSuspender suspender;
    if (batchesPerBase <= 0) {
      batchesPerBase = numVectorsPerThread;
    }

    // Bases live on the benchmark's pool; their BufferPtrs are
    // shared across worker threads via wrap.
    auto bases = buildBases<BaseNativeType>(
        baseType,
        distinctValueCount,
        numVectorsPerThread,
        batchesPerBase,
        pool());
    auto rowType = ROW({"c0"}, {baseType});

    struct ThreadCtx {
      std::shared_ptr<memory::MemoryPool> pool;
      std::shared_ptr<core::QueryCtx> queryCtx;
      std::unique_ptr<core::ExecCtx> execCtx;
      std::unique_ptr<exec::ExprSet> exprSet;
      std::vector<RowVectorPtr> inputs;
    };
    std::vector<ThreadCtx> threadCtxs(numThreads);

    for (int32_t t = 0; t < numThreads; ++t) {
      auto& tc = threadCtxs[t];
      tc.pool = memory::memoryManager()->addLeafPool();
      tc.queryCtx = core::QueryCtx::create();
      tc.execCtx =
          std::make_unique<core::ExecCtx>(tc.pool.get(), tc.queryCtx.get());
      facebook::velox::test::VectorMaker maker{tc.pool.get()};

      tc.inputs.reserve(numVectorsPerThread);
      for (int32_t vectorIdx = 0; vectorIdx < numVectorsPerThread;
           ++vectorIdx) {
        const int32_t offset = vectorIdx * newIndicesPerVector;
        const int32_t baseIdx = vectorIdx / batchesPerBase;
        auto dict = wrapInDict(
            bases[baseIdx],
            offset,
            rowsPerVector,
            distinctValueCount,
            nullPct,
            tc.pool.get());
        tc.inputs.push_back(maker.rowVector({dict}));
      }

      // Compile per worker so each holds its own dictionaryCache_.
      auto untyped =
          parse::DuckSqlExpressionsParser(options_).parseExpr(expression);
      auto typed = core::Expressions::inferTypes(
          untyped, rowType, tc.execCtx->pool());
      std::vector<core::TypedExprPtr> typedExprs{typed};
      tc.exprSet =
          std::make_unique<exec::ExprSet>(typedExprs, tc.execCtx.get());
    }
    suspender.dismiss();

    std::atomic<size_t> totalCount{0};
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (int32_t t = 0; t < numThreads; ++t) {
      threads.emplace_back([&threadCtxs, &totalCount, t]() {
        auto& tc = threadCtxs[t];
        size_t count = 0;
        for (auto& input : tc.inputs) {
          SelectivityVector rows(input->size());
          exec::EvalCtx evalCtx(
              tc.execCtx.get(), tc.exprSet.get(), input.get());
          std::vector<VectorPtr> results{nullptr};
          tc.exprSet->eval(rows, evalCtx, results);
          folly::doNotOptimizeAway(results);
          count += results[0]->size();
        }
        totalCount.fetch_add(count, std::memory_order_relaxed);
      });
    }
    for (auto& th : threads) {
      th.join();
    }
    return totalCount.load();
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

 private:
  // Builds the set of distinct base FlatVectors that the dictionary
  // wraps rotate through. With `batchesPerBase >= numVectors`, this
  // returns a single base (matching the original behavior). Each
  // base is populated with values starting at `baseIdx *
  // distinctValueCount + 1` so distinct bases have distinct content
  // (which makes peelEncodings register a real base change on
  // rotation, not just a fresh pointer to the same content).
  template <typename BaseNativeType>
  std::vector<VectorPtr> buildBases(
      const TypePtr& baseType,
      int32_t distinctValueCount,
      int32_t numVectors,
      int32_t batchesPerBase,
      memory::MemoryPool* basePool) {
    const int32_t numBases =
        std::max(1, (numVectors + batchesPerBase - 1) / batchesPerBase);
    facebook::velox::test::VectorMaker maker{basePool};
    std::vector<VectorPtr> bases;
    bases.reserve(numBases);
    for (int32_t b = 0; b < numBases; ++b) {
      const int32_t baseOffset = b * distinctValueCount;
      bases.push_back(maker.flatVector<BaseNativeType>(
          distinctValueCount,
          [baseOffset](vector_size_t row) {
            return static_cast<BaseNativeType>(row + baseOffset + 1);
          },
          nullptr,
          baseType));
    }
    return bases;
  }

  // Builds a DictionaryVector over `base` of `rowsPerVector` rows.
  // The indices walk `(row + offset) mod distinctValueCount`, so
  // `offset` shifts the visible slice of the base across batches.
  // `nullPct` controls how many of the dictionary positions are
  // marked null on the wrap (not on the base).
  VectorPtr wrapInDict(
      const VectorPtr& base,
      int32_t offset,
      int32_t rowsPerVector,
      int32_t distinctValueCount,
      int32_t nullPct,
      memory::MemoryPool* dictPool) {
    auto indices =
        AlignedBuffer::allocate<vector_size_t>(rowsPerVector, dictPool);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (int32_t row = 0; row < rowsPerVector; ++row) {
      rawIndices[row] = (row + offset) % distinctValueCount;
    }

    BufferPtr nulls;
    if (nullPct > 0) {
      nulls = AlignedBuffer::allocate<bool>(rowsPerVector, dictPool);
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
    return BaseVector::wrapInDictionary(nulls, indices, rowsPerVector, base);
  }
};

// === Single-base DICT free functions. Each loops the work `iters`
// times so folly's iteration-scaling controls measurement length.
// batchesPerBase is passed as a runtime parameter so the same free
// function can drive both single-base and multi-base entries.

unsigned DICT_BigintToVarchar(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct,
    int32_t batchesPerBase) {
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
        nullPct,
        batchesPerBase);
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
    int32_t nullPct,
    int32_t batchesPerBase) {
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
        nullPct,
        batchesPerBase);
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
    int32_t nullPct,
    int32_t batchesPerBase) {
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
        nullPct,
        batchesPerBase);
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
    int32_t nullPct,
    int32_t batchesPerBase) {
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
        nullPct,
        batchesPerBase);
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
    int32_t nullPct,
    int32_t batchesPerBase) {
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
        nullPct,
        batchesPerBase);
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

// === Production date_format expression
//
// `date_format(CAST(date_trunc('day', date_add('day',
//    0 - (((day_of_week(c0) % 7) - 1 + 7) % 7), c0)) AS timestamp),
//  '%Y-%m-%d')`
//
// Input is DATE, output is VARCHAR. This is the exact shape that
// drove the original profile - the result Buffer is shared across
// batches via dictionaryCache_, and the per-batch copy iterates its
// stringBuffers_ with one atomic refcount increment per Buffer.

unsigned DICT_DateFormatProd(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct,
    int32_t batchesPerBase) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionary<int32_t>(
        kDateFormatProdExpr,
        DATE(),
        numVectors,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct,
        batchesPerBase);
  }
  return total;
}

unsigned FLAT_DateFormatProd(
    unsigned iters,
    int32_t numVectors,
    int32_t rowsPerVector,
    int32_t nullPct) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runFlat<int32_t>(
        kDateFormatProdExpr, DATE(), numVectors, rowsPerVector, nullPct);
  }
  return total;
}

// === Multi-thread DICT free functions. The first runtime arg is the
// number of worker threads; the second is per-thread numVectors.

unsigned MT_DICT_BigintToVarchar(
    unsigned iters,
    int32_t numThreads,
    int32_t numVectorsPerThread,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct,
    int32_t batchesPerBase) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionaryMultiThread<int64_t>(
        "cast(c0 as varchar)",
        BIGINT(),
        numThreads,
        numVectorsPerThread,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct,
        batchesPerBase);
  }
  return total;
}

unsigned MT_DICT_DateToVarchar(
    unsigned iters,
    int32_t numThreads,
    int32_t numVectorsPerThread,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct,
    int32_t batchesPerBase) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionaryMultiThread<int32_t>(
        "cast(c0 as varchar)",
        DATE(),
        numThreads,
        numVectorsPerThread,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct,
        batchesPerBase);
  }
  return total;
}

unsigned MT_DICT_DateToTimestamp(
    unsigned iters,
    int32_t numThreads,
    int32_t numVectorsPerThread,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct,
    int32_t batchesPerBase) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionaryMultiThread<int32_t>(
        "cast(c0 as timestamp)",
        DATE(),
        numThreads,
        numVectorsPerThread,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct,
        batchesPerBase);
  }
  return total;
}

unsigned MT_DICT_DateFormatProd(
    unsigned iters,
    int32_t numThreads,
    int32_t numVectorsPerThread,
    int32_t rowsPerVector,
    int32_t distinctValueCount,
    int32_t newIndicesPerVector,
    int32_t nullPct,
    int32_t batchesPerBase) {
  WideningCastBenchmark benchmark;
  unsigned total = 0;
  for (unsigned i = 0; i < iters; ++i) {
    total += benchmark.runDictionaryMultiThread<int32_t>(
        kDateFormatProdExpr,
        DATE(),
        numThreads,
        numVectorsPerThread,
        rowsPerVector,
        distinctValueCount,
        newIndicesPerVector,
        nullPct,
        batchesPerBase);
  }
  return total;
}

} // namespace

// Each macro pastes the parameter values into the entry name so the
// table is grep-able. The constant `1000` literal in DICT/FLAT calls
// is numVectors (also baked into the entry name format described in
// READING THE BENCHMARK OUTPUT above). `1000` literal in DICT calls
// for batchesPerBase makes the cache cover the entire iteration -
// the original "single base" behavior. The `_bpb<n>` suffix opts in
// to the multi-base behavior.

#define DICT(                                                              \
    funcName,                                                              \
    rowsPerVector,                                                         \
    distinctValueCount,                                                    \
    newIndicesPerVector,                                                   \
    nullPct)                                                               \
  BENCHMARK_NAMED_PARAM_MULTI(                                             \
      funcName,                                                            \
      rowsPerVector##_##distinctValueCount##_##newIndicesPerVector##_##nullPct, \
      1000,                                                                \
      rowsPerVector,                                                       \
      distinctValueCount,                                                  \
      newIndicesPerVector,                                                 \
      nullPct,                                                             \
      1000)

#define DICT_NULLS(                                                   \
    funcName, rowsPerVector, distinctValueCount, newIndicesPerVector) \
  DICT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 0)  \
  DICT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 50) \
  DICT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 100)

// Multi-base variant. `batchesPerBase` is the number of consecutive
// batches that reuse the same base FlatVector before rotating.
#define DICT_BPB(                                                          \
    funcName,                                                              \
    rowsPerVector,                                                         \
    distinctValueCount,                                                    \
    newIndicesPerVector,                                                   \
    nullPct,                                                               \
    batchesPerBase)                                                        \
  BENCHMARK_NAMED_PARAM_MULTI(                                             \
      funcName,                                                            \
      rowsPerVector##_##distinctValueCount##_##newIndicesPerVector##_##nullPct##_bpb##batchesPerBase, \
      1000,                                                                \
      rowsPerVector,                                                       \
      distinctValueCount,                                                  \
      newIndicesPerVector,                                                 \
      nullPct,                                                             \
      batchesPerBase)

// Sweep batchesPerBase across {1, 10, 100} for a given config. With
// numVectors=1000, that is {1000, 100, 10} base changes per call.
#define DICT_ALT(                                                     \
    funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct) \
  DICT_BPB(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct, 1) \
  DICT_BPB(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct, 10) \
  DICT_BPB(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct, 100)

#define DICT_ALT_NULLS(                                               \
    funcName, rowsPerVector, distinctValueCount, newIndicesPerVector) \
  DICT_ALT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 0) \
  DICT_ALT(funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, 50)

#define FLAT(funcName, rowsPerVector, nullPct)         \
  BENCHMARK_NAMED_PARAM_MULTI(                         \
      funcName, rowsPerVector##_##nullPct, 1000, rowsPerVector, nullPct)

#define FLAT_NULLS(funcName, rowsPerVector) \
  FLAT(funcName, rowsPerVector, 0)          \
  FLAT(funcName, rowsPerVector, 50)         \
  FLAT(funcName, rowsPerVector, 100)

// Multi-thread macro. Uses 1000 as the per-thread numVectors literal.
#define MT(                                                                \
    funcName,                                                              \
    numThreads,                                                            \
    rowsPerVector,                                                         \
    distinctValueCount,                                                    \
    newIndicesPerVector,                                                   \
    nullPct,                                                               \
    batchesPerBase)                                                        \
  BENCHMARK_NAMED_PARAM_MULTI(                                             \
      funcName,                                                            \
      threads##numThreads##_##rowsPerVector##_##distinctValueCount##_##newIndicesPerVector##_##nullPct##_bpb##batchesPerBase, \
      numThreads,                                                          \
      1000,                                                                \
      rowsPerVector,                                                       \
      distinctValueCount,                                                  \
      newIndicesPerVector,                                                 \
      nullPct,                                                             \
      batchesPerBase)

// MT_ALT: sweep batchesPerBase {1, 100, 1000} for given thread count
// and config. Lower batchesPerBase amplifies contention because each
// batch starts a fresh evalWithMemo path, repeatedly touching the
// source Buffer's refcount line across threads.
#define MT_ALT(                                                       \
    funcName,                                                         \
    numThreads,                                                       \
    rowsPerVector,                                                    \
    distinctValueCount,                                               \
    newIndicesPerVector,                                              \
    nullPct)                                                          \
  MT(funcName, numThreads, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct, 1) \
  MT(funcName, numThreads, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct, 100) \
  MT(funcName, numThreads, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct, 1000)

// MT_THREADS: sweep thread counts for a fixed dict config, sweeping
// batchesPerBase within each thread count.
#define MT_THREADS(                                                   \
    funcName, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct) \
  MT_ALT(funcName, 4, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct) \
  MT_ALT(funcName, 16, rowsPerVector, distinctValueCount, newIndicesPerVector, nullPct)

// === Existing single-base entries (batchesPerBase = numVectors = 1000)
// kept as a "best-case cache hit" baseline.

#define DICT_BIGINT_TO_VARCHAR                          \
  DICT_NULLS(DICT_BigintToVarchar, 100, 5, 1)          \
  DICT_NULLS(DICT_BigintToVarchar, 100, 5, 100)        \
  DICT_NULLS(DICT_BigintToVarchar, 100, 5, 1000)       \
  DICT_NULLS(DICT_BigintToVarchar, 100, 500, 1)        \
  DICT_NULLS(DICT_BigintToVarchar, 100, 500, 100)      \
  DICT_NULLS(DICT_BigintToVarchar, 100, 500, 1000)     \
  DICT_NULLS(DICT_BigintToVarchar, 100, 15000, 1)      \
  DICT_NULLS(DICT_BigintToVarchar, 100, 15000, 100)    \
  DICT_NULLS(DICT_BigintToVarchar, 100, 15000, 1000)   \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 5, 1)         \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 5, 100)       \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 5, 1000)      \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 500, 1)       \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 500, 100)     \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 500, 1000)    \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 15000, 1)     \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 15000, 100)   \
  DICT_NULLS(DICT_BigintToVarchar, 1000, 15000, 1000)  \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 5, 1)        \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 5, 100)      \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 5, 1000)     \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 500, 1)      \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 500, 100)    \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 500, 1000)   \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 15000, 1)    \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 15000, 100)  \
  DICT_NULLS(DICT_BigintToVarchar, 10000, 15000, 1000)

#define FLAT_BIGINT_TO_VARCHAR              \
  FLAT_NULLS(FLAT_BigintToVarchar, 100)     \
  FLAT_NULLS(FLAT_BigintToVarchar, 1000)    \
  FLAT_NULLS(FLAT_BigintToVarchar, 10000)

#define DICT_INT_TO_BIGINT                              \
  DICT_NULLS(DICT_IntToBigint, 100, 5, 1)              \
  DICT_NULLS(DICT_IntToBigint, 100, 5, 100)            \
  DICT_NULLS(DICT_IntToBigint, 100, 5, 1000)           \
  DICT_NULLS(DICT_IntToBigint, 100, 500, 1)            \
  DICT_NULLS(DICT_IntToBigint, 100, 500, 100)          \
  DICT_NULLS(DICT_IntToBigint, 100, 500, 1000)         \
  DICT_NULLS(DICT_IntToBigint, 100, 15000, 1)          \
  DICT_NULLS(DICT_IntToBigint, 100, 15000, 100)        \
  DICT_NULLS(DICT_IntToBigint, 100, 15000, 1000)       \
  DICT_NULLS(DICT_IntToBigint, 1000, 5, 1)             \
  DICT_NULLS(DICT_IntToBigint, 1000, 5, 100)           \
  DICT_NULLS(DICT_IntToBigint, 1000, 5, 1000)          \
  DICT_NULLS(DICT_IntToBigint, 1000, 500, 1)           \
  DICT_NULLS(DICT_IntToBigint, 1000, 500, 100)         \
  DICT_NULLS(DICT_IntToBigint, 1000, 500, 1000)        \
  DICT_NULLS(DICT_IntToBigint, 1000, 15000, 1)         \
  DICT_NULLS(DICT_IntToBigint, 1000, 15000, 100)       \
  DICT_NULLS(DICT_IntToBigint, 1000, 15000, 1000)      \
  DICT_NULLS(DICT_IntToBigint, 10000, 5, 1)            \
  DICT_NULLS(DICT_IntToBigint, 10000, 5, 100)          \
  DICT_NULLS(DICT_IntToBigint, 10000, 5, 1000)         \
  DICT_NULLS(DICT_IntToBigint, 10000, 500, 1)          \
  DICT_NULLS(DICT_IntToBigint, 10000, 500, 100)        \
  DICT_NULLS(DICT_IntToBigint, 10000, 500, 1000)       \
  DICT_NULLS(DICT_IntToBigint, 10000, 15000, 1)        \
  DICT_NULLS(DICT_IntToBigint, 10000, 15000, 100)      \
  DICT_NULLS(DICT_IntToBigint, 10000, 15000, 1000)

#define FLAT_INT_TO_BIGINT                  \
  FLAT_NULLS(FLAT_IntToBigint, 100)         \
  FLAT_NULLS(FLAT_IntToBigint, 1000)        \
  FLAT_NULLS(FLAT_IntToBigint, 10000)

#define DICT_DATE_TO_VARCHAR                            \
  DICT_NULLS(DICT_DateToVarchar, 100, 5, 1)            \
  DICT_NULLS(DICT_DateToVarchar, 100, 5, 100)          \
  DICT_NULLS(DICT_DateToVarchar, 100, 5, 1000)         \
  DICT_NULLS(DICT_DateToVarchar, 100, 500, 1)          \
  DICT_NULLS(DICT_DateToVarchar, 100, 500, 100)        \
  DICT_NULLS(DICT_DateToVarchar, 100, 500, 1000)       \
  DICT_NULLS(DICT_DateToVarchar, 100, 15000, 1)        \
  DICT_NULLS(DICT_DateToVarchar, 100, 15000, 100)      \
  DICT_NULLS(DICT_DateToVarchar, 100, 15000, 1000)     \
  DICT_NULLS(DICT_DateToVarchar, 1000, 5, 1)           \
  DICT_NULLS(DICT_DateToVarchar, 1000, 5, 100)         \
  DICT_NULLS(DICT_DateToVarchar, 1000, 5, 1000)        \
  DICT_NULLS(DICT_DateToVarchar, 1000, 500, 1)         \
  DICT_NULLS(DICT_DateToVarchar, 1000, 500, 100)       \
  DICT_NULLS(DICT_DateToVarchar, 1000, 500, 1000)      \
  DICT_NULLS(DICT_DateToVarchar, 1000, 15000, 1)       \
  DICT_NULLS(DICT_DateToVarchar, 1000, 15000, 100)     \
  DICT_NULLS(DICT_DateToVarchar, 1000, 15000, 1000)    \
  DICT_NULLS(DICT_DateToVarchar, 10000, 5, 1)          \
  DICT_NULLS(DICT_DateToVarchar, 10000, 5, 100)        \
  DICT_NULLS(DICT_DateToVarchar, 10000, 5, 1000)       \
  DICT_NULLS(DICT_DateToVarchar, 10000, 500, 1)        \
  DICT_NULLS(DICT_DateToVarchar, 10000, 500, 100)      \
  DICT_NULLS(DICT_DateToVarchar, 10000, 500, 1000)     \
  DICT_NULLS(DICT_DateToVarchar, 10000, 15000, 1)      \
  DICT_NULLS(DICT_DateToVarchar, 10000, 15000, 100)    \
  DICT_NULLS(DICT_DateToVarchar, 10000, 15000, 1000)

#define FLAT_DATE_TO_VARCHAR                \
  FLAT_NULLS(FLAT_DateToVarchar, 100)       \
  FLAT_NULLS(FLAT_DateToVarchar, 1000)      \
  FLAT_NULLS(FLAT_DateToVarchar, 10000)

#define DICT_DATE_TO_TIMESTAMP                          \
  DICT_NULLS(DICT_DateToTimestamp, 100, 5, 1)          \
  DICT_NULLS(DICT_DateToTimestamp, 100, 5, 100)        \
  DICT_NULLS(DICT_DateToTimestamp, 100, 5, 1000)       \
  DICT_NULLS(DICT_DateToTimestamp, 100, 500, 1)        \
  DICT_NULLS(DICT_DateToTimestamp, 100, 500, 100)      \
  DICT_NULLS(DICT_DateToTimestamp, 100, 500, 1000)     \
  DICT_NULLS(DICT_DateToTimestamp, 100, 15000, 1)      \
  DICT_NULLS(DICT_DateToTimestamp, 100, 15000, 100)    \
  DICT_NULLS(DICT_DateToTimestamp, 100, 15000, 1000)   \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 5, 1)         \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 5, 100)       \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 5, 1000)      \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 500, 1)       \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 500, 100)     \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 500, 1000)    \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 15000, 1)     \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 15000, 100)   \
  DICT_NULLS(DICT_DateToTimestamp, 1000, 15000, 1000)  \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 5, 1)        \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 5, 100)      \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 5, 1000)     \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 500, 1)      \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 500, 100)    \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 500, 1000)   \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 15000, 1)    \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 15000, 100)  \
  DICT_NULLS(DICT_DateToTimestamp, 10000, 15000, 1000)

#define FLAT_DATE_TO_TIMESTAMP              \
  FLAT_NULLS(FLAT_DateToTimestamp, 100)     \
  FLAT_NULLS(FLAT_DateToTimestamp, 1000)    \
  FLAT_NULLS(FLAT_DateToTimestamp, 10000)

#define DICT_REAL_TO_DOUBLE                             \
  DICT_NULLS(DICT_RealToDouble, 100, 5, 1)             \
  DICT_NULLS(DICT_RealToDouble, 100, 5, 100)           \
  DICT_NULLS(DICT_RealToDouble, 100, 5, 1000)          \
  DICT_NULLS(DICT_RealToDouble, 100, 500, 1)           \
  DICT_NULLS(DICT_RealToDouble, 100, 500, 100)         \
  DICT_NULLS(DICT_RealToDouble, 100, 500, 1000)        \
  DICT_NULLS(DICT_RealToDouble, 100, 15000, 1)         \
  DICT_NULLS(DICT_RealToDouble, 100, 15000, 100)       \
  DICT_NULLS(DICT_RealToDouble, 100, 15000, 1000)      \
  DICT_NULLS(DICT_RealToDouble, 1000, 5, 1)            \
  DICT_NULLS(DICT_RealToDouble, 1000, 5, 100)          \
  DICT_NULLS(DICT_RealToDouble, 1000, 5, 1000)         \
  DICT_NULLS(DICT_RealToDouble, 1000, 500, 1)          \
  DICT_NULLS(DICT_RealToDouble, 1000, 500, 100)        \
  DICT_NULLS(DICT_RealToDouble, 1000, 500, 1000)       \
  DICT_NULLS(DICT_RealToDouble, 1000, 15000, 1)        \
  DICT_NULLS(DICT_RealToDouble, 1000, 15000, 100)      \
  DICT_NULLS(DICT_RealToDouble, 1000, 15000, 1000)     \
  DICT_NULLS(DICT_RealToDouble, 10000, 5, 1)           \
  DICT_NULLS(DICT_RealToDouble, 10000, 5, 100)         \
  DICT_NULLS(DICT_RealToDouble, 10000, 5, 1000)        \
  DICT_NULLS(DICT_RealToDouble, 10000, 500, 1)         \
  DICT_NULLS(DICT_RealToDouble, 10000, 500, 100)       \
  DICT_NULLS(DICT_RealToDouble, 10000, 500, 1000)      \
  DICT_NULLS(DICT_RealToDouble, 10000, 15000, 1)       \
  DICT_NULLS(DICT_RealToDouble, 10000, 15000, 100)     \
  DICT_NULLS(DICT_RealToDouble, 10000, 15000, 1000)

#define FLAT_REAL_TO_DOUBLE                 \
  FLAT_NULLS(FLAT_RealToDouble, 100)        \
  FLAT_NULLS(FLAT_RealToDouble, 1000)       \
  FLAT_NULLS(FLAT_RealToDouble, 10000)

// === Multi-base alternation entries. Production scans rarely stay on
// one base for 1000 batches - the underlying storage chunk advances
// every batch or every handful of batches. Sweep the realistic
// regime by picking a few high-signal shapes per type pair.

#define DICT_ALT_BIGINT_TO_VARCHAR                                  \
  DICT_ALT_NULLS(DICT_BigintToVarchar, 1000, 500, 1)                \
  DICT_ALT_NULLS(DICT_BigintToVarchar, 1000, 15000, 1)              \
  DICT_ALT_NULLS(DICT_BigintToVarchar, 10000, 500, 1)               \
  DICT_ALT_NULLS(DICT_BigintToVarchar, 10000, 15000, 1)

#define DICT_ALT_INT_TO_BIGINT                                      \
  DICT_ALT_NULLS(DICT_IntToBigint, 1000, 500, 1)                    \
  DICT_ALT_NULLS(DICT_IntToBigint, 1000, 15000, 1)                  \
  DICT_ALT_NULLS(DICT_IntToBigint, 10000, 500, 1)                   \
  DICT_ALT_NULLS(DICT_IntToBigint, 10000, 15000, 1)

#define DICT_ALT_DATE_TO_VARCHAR                                    \
  DICT_ALT_NULLS(DICT_DateToVarchar, 1000, 500, 1)                  \
  DICT_ALT_NULLS(DICT_DateToVarchar, 1000, 15000, 1)                \
  DICT_ALT_NULLS(DICT_DateToVarchar, 10000, 500, 1)                 \
  DICT_ALT_NULLS(DICT_DateToVarchar, 10000, 15000, 1)

#define DICT_ALT_DATE_TO_TIMESTAMP                                  \
  DICT_ALT_NULLS(DICT_DateToTimestamp, 1000, 500, 1)                \
  DICT_ALT_NULLS(DICT_DateToTimestamp, 1000, 15000, 1)              \
  DICT_ALT_NULLS(DICT_DateToTimestamp, 10000, 500, 1)               \
  DICT_ALT_NULLS(DICT_DateToTimestamp, 10000, 15000, 1)

#define DICT_ALT_REAL_TO_DOUBLE                                     \
  DICT_ALT_NULLS(DICT_RealToDouble, 1000, 500, 1)                   \
  DICT_ALT_NULLS(DICT_RealToDouble, 1000, 15000, 1)                 \
  DICT_ALT_NULLS(DICT_RealToDouble, 10000, 500, 1)                  \
  DICT_ALT_NULLS(DICT_RealToDouble, 10000, 15000, 1)

// === Production date_format expression (DATE -> VARCHAR).

#define DICT_DATE_FORMAT_PROD                                       \
  DICT_NULLS(DICT_DateFormatProd, 1000, 500, 1)                     \
  DICT_NULLS(DICT_DateFormatProd, 1000, 15000, 1)                   \
  DICT_NULLS(DICT_DateFormatProd, 10000, 500, 1)                    \
  DICT_NULLS(DICT_DateFormatProd, 10000, 15000, 1)                  \
  DICT_ALT_NULLS(DICT_DateFormatProd, 1000, 500, 1)                 \
  DICT_ALT_NULLS(DICT_DateFormatProd, 1000, 15000, 1)               \
  DICT_ALT_NULLS(DICT_DateFormatProd, 10000, 500, 1)                \
  DICT_ALT_NULLS(DICT_DateFormatProd, 10000, 15000, 1)

#define FLAT_DATE_FORMAT_PROD                                       \
  FLAT_NULLS(FLAT_DateFormatProd, 1000)                             \
  FLAT_NULLS(FLAT_DateFormatProd, 10000)

// === Multi-thread entries. Threads share the source base
// FlatVector(s) - cross-driver atomic refcount on the source Buffer
// is exactly the contention pattern observed in production.

#define MT_DICT_BIGINT_TO_VARCHAR                                   \
  MT_THREADS(MT_DICT_BigintToVarchar, 1000, 500, 1, 0)              \
  MT_THREADS(MT_DICT_BigintToVarchar, 1000, 15000, 1, 0)            \
  MT_THREADS(MT_DICT_BigintToVarchar, 10000, 500, 1, 0)

#define MT_DICT_DATE_TO_VARCHAR                                     \
  MT_THREADS(MT_DICT_DateToVarchar, 1000, 500, 1, 0)                \
  MT_THREADS(MT_DICT_DateToVarchar, 1000, 15000, 1, 0)              \
  MT_THREADS(MT_DICT_DateToVarchar, 10000, 500, 1, 0)

#define MT_DICT_DATE_TO_TIMESTAMP                                   \
  MT_THREADS(MT_DICT_DateToTimestamp, 1000, 500, 1, 0)              \
  MT_THREADS(MT_DICT_DateToTimestamp, 1000, 15000, 1, 0)            \
  MT_THREADS(MT_DICT_DateToTimestamp, 10000, 500, 1, 0)

#define MT_DICT_DATE_FORMAT_PROD                                    \
  MT_THREADS(MT_DICT_DateFormatProd, 1000, 500, 1, 0)               \
  MT_THREADS(MT_DICT_DateFormatProd, 1000, 15000, 1, 0)             \
  MT_THREADS(MT_DICT_DateFormatProd, 10000, 500, 1, 0)

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
BENCHMARK_DRAW_LINE();
DICT_ALT_BIGINT_TO_VARCHAR
BENCHMARK_DRAW_LINE();
DICT_ALT_INT_TO_BIGINT
BENCHMARK_DRAW_LINE();
DICT_ALT_DATE_TO_VARCHAR
BENCHMARK_DRAW_LINE();
DICT_ALT_DATE_TO_TIMESTAMP
BENCHMARK_DRAW_LINE();
DICT_ALT_REAL_TO_DOUBLE
BENCHMARK_DRAW_LINE();
DICT_DATE_FORMAT_PROD
FLAT_DATE_FORMAT_PROD
BENCHMARK_DRAW_LINE();
MT_DICT_BIGINT_TO_VARCHAR
BENCHMARK_DRAW_LINE();
MT_DICT_DATE_TO_VARCHAR
BENCHMARK_DRAW_LINE();
MT_DICT_DATE_TO_TIMESTAMP
BENCHMARK_DRAW_LINE();
MT_DICT_DATE_FORMAT_PROD

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  std::cout
      << "\nBenchmark entry names encode the sweep parameters:\n"
      << "  DICT_<from>To<to>("
         "rowsPerVector_distinctValueCount_newIndicesPerVector_nullPct[_bpb<batchesPerBase>])\n"
      << "  FLAT_<from>To<to>(rowsPerVector_nullPct)\n"
      << "  MT_DICT_<from>To<to>("
         "threads<n>_rowsPerVector_distinctValueCount_newIndicesPerVector_nullPct_bpb<batchesPerBase>)\n"
      << "\n"
      << "numVectors is fixed at 1000 for every entry (per worker thread for\n"
      << "MT entries) and is not encoded in the name. Each measurement runs\n"
      << "the cast over 1000 input vectors back-to-back; the reported\n"
      << "time/iter is amortized over the total row count\n"
      << "(numThreads * 1000 * rowsPerVector for MT entries).\n"
      << "\n"
      << "  rowsPerVector       : rows in each input vector\n"
      << "  distinctValueCount  : dictionary base cardinality (DICT only)\n"
      << "  newIndicesPerVector : new dictionary indices each input vector\n"
      << "                        introduces vs the previous one - drives\n"
      << "                        the dictionary-cache miss rate against\n"
      << "                        a stable base (DICT only)\n"
      << "  nullPct             : percentage of positions marked null in\n"
      << "                        {0, 50, 100}. For DICT, nulls live on\n"
      << "                        the dictionary wrap; for FLAT, on the\n"
      << "                        flat input itself.\n"
      << "  batchesPerBase      : how many consecutive batches reuse the\n"
      << "                        same base FlatVector before rotating to\n"
      << "                        a different base (different BufferPtr,\n"
      << "                        different content). Entries without\n"
      << "                        _bpb<n> use a single base for the whole\n"
      << "                        run (= 1000). _bpb1 / _bpb10 / _bpb100\n"
      << "                        sweep the realistic mix of base-change\n"
      << "                        events. Lower means more\n"
      << "                        numMemoBaseChange events per call.\n"
      << "  threads<n>          : MT entries only. Number of worker threads\n"
      << "                        all evaluating against the shared base\n"
      << "                        FlatVectors - reproduces cross-driver\n"
      << "                        atomic refcount contention on source\n"
      << "                        Buffers.\n"
      << "\n";

  folly::runBenchmarks();
  return 0;
}
