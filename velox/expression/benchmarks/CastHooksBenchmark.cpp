/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Copyright (c) 2026 IBM Corporation.
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
#include <folly/init/Init.h>

#include "velox/expression/Expr.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// Compares the V1 cast path (per-row PrestoCastHooks scalar dispatch
// inside CastExpr's row loop) against the V2 cast path (whole-column
// PrestoCastHooksV2 dispatch inside CastExprV2).
//
// Both paths execute the exact same expression text but pick a
// different evaluator via the expression.eval_v2 config.  The
// underlying scalar conversion is identical in both directions, so any
// difference comes from the per-row virtual dispatch vs. whole-column
// dispatch shape.
//
// Five cast directions exercised:
//   - VARCHAR -> TIMESTAMP
//   - VARCHAR -> DATE
//   - VARCHAR -> REAL
//   - VARCHAR -> DOUBLE
//   - DATE    -> TIMESTAMP

using namespace facebook::velox;

namespace {

class CastHooksBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  static constexpr vector_size_t kSize = 10'000;

  CastHooksBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerAllScalarFunctions();
  }

  // Toggles which evaluator the benchmark uses for subsequent
  // compileExpression calls.  V1 is the default; V2 routes through
  // ExprSetV2 + CastExprV2 + PrestoCastHooksV2.
  void useV2Evaluator(bool enabled) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kExprEvalV2, enabled ? "true" : "false"}});
  }

  RowVectorPtr stringTimestampInput() {
    auto column = vectorMaker_.flatVector<StringView>(
        kSize,
        [](vector_size_t row) {
          // Vary across rows so the parser can't cache anything.
          static const std::array<const char*, 4> patterns = {
              "2024-01-15 12:34:56",
              "2024-06-01 00:00:00",
              "1970-01-01 00:00:00",
              "2099-12-31 23:59:59",
          };
          return StringView(patterns[row & 3]);
        });
    return vectorMaker_.rowVector({"s"}, {column});
  }

  RowVectorPtr stringDateInput() {
    auto column = vectorMaker_.flatVector<StringView>(
        kSize,
        [](vector_size_t row) {
          static const std::array<const char*, 4> patterns = {
              "2024-01-15",
              "2024-06-01",
              "1970-01-01",
              "2099-12-31",
          };
          return StringView(patterns[row & 3]);
        });
    return vectorMaker_.rowVector({"s"}, {column});
  }

  RowVectorPtr stringFloatingInput() {
    auto column = vectorMaker_.flatVector<StringView>(
        kSize,
        [](vector_size_t row) {
          static const std::array<const char*, 8> patterns = {
              "3.14",
              "0.0",
              "-1.5",
              "1e10",
              "2.71828",
              "-1234.5678",
              "1.0",
              "9.999",
          };
          return StringView(patterns[row & 7]);
        });
    return vectorMaker_.rowVector({"s"}, {column});
  }

  RowVectorPtr dateInput() {
    auto column = vectorMaker_.flatVector<int32_t>(
        kSize, [](vector_size_t row) { return 19'737 + (row & 1023); }, nullptr, DATE());
    return vectorMaker_.rowVector({"d"}, {column});
  }

  size_t runCast(const std::string& expression, const RowVectorPtr& rowVector) {
    folly::BenchmarkSuspender suspender;
    // Bypass FunctionBenchmarkBase::compileExpression (which constructs
    // exec::ExprSet directly and ignores the flag) so useV2Evaluator
    // actually routes evaluation through ExprSetV2 when on.
    auto untyped =
        parse::DuckSqlExpressionsParser(options_).parseExpr(expression);
    auto typed = core::Expressions::inferTypes(
        untyped, rowVector->type(), execCtx_.pool());
    std::vector<core::TypedExprPtr> typedExprs{typed};
    auto exprSet = exec::makeExprSetFromFlag(std::move(typedExprs), &execCtx_);
    suspender.dismiss();

    int total = 0;
    for (int i = 0; i < 100; ++i) {
      total += evaluate(*exprSet, rowVector)->size();
    }
    return total;
  }
};

// VARCHAR -> TIMESTAMP

BENCHMARK_MULTI(varcharToTimestamp_v1) {
  CastHooksBenchmark b;
  b.useV2Evaluator(false);
  return b.runCast("cast(s as timestamp)", b.stringTimestampInput());
}

BENCHMARK_RELATIVE_MULTI(varcharToTimestamp_v2) {
  CastHooksBenchmark b;
  b.useV2Evaluator(true);
  return b.runCast("cast(s as timestamp)", b.stringTimestampInput());
}

// VARCHAR -> DATE

BENCHMARK_MULTI(varcharToDate_v1) {
  CastHooksBenchmark b;
  b.useV2Evaluator(false);
  return b.runCast("cast(s as date)", b.stringDateInput());
}

BENCHMARK_RELATIVE_MULTI(varcharToDate_v2) {
  CastHooksBenchmark b;
  b.useV2Evaluator(true);
  return b.runCast("cast(s as date)", b.stringDateInput());
}

// VARCHAR -> REAL

BENCHMARK_MULTI(varcharToReal_v1) {
  CastHooksBenchmark b;
  b.useV2Evaluator(false);
  return b.runCast("cast(s as real)", b.stringFloatingInput());
}

BENCHMARK_RELATIVE_MULTI(varcharToReal_v2) {
  CastHooksBenchmark b;
  b.useV2Evaluator(true);
  return b.runCast("cast(s as real)", b.stringFloatingInput());
}

// VARCHAR -> DOUBLE

BENCHMARK_MULTI(varcharToDouble_v1) {
  CastHooksBenchmark b;
  b.useV2Evaluator(false);
  return b.runCast("cast(s as double)", b.stringFloatingInput());
}

BENCHMARK_RELATIVE_MULTI(varcharToDouble_v2) {
  CastHooksBenchmark b;
  b.useV2Evaluator(true);
  return b.runCast("cast(s as double)", b.stringFloatingInput());
}

// DATE -> TIMESTAMP

BENCHMARK_MULTI(dateToTimestamp_v1) {
  CastHooksBenchmark b;
  b.useV2Evaluator(false);
  return b.runCast("cast(d as timestamp)", b.dateInput());
}

BENCHMARK_RELATIVE_MULTI(dateToTimestamp_v2) {
  CastHooksBenchmark b;
  b.useV2Evaluator(true);
  return b.runCast("cast(d as timestamp)", b.dateInput());
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
