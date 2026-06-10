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

#include <gtest/gtest.h>

#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprSetV2.h"
#include "velox/expression/ExprV2.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
namespace {

/// Compares V1 (ExprSet) and V2 (ExprSetV2) on a restricted subset of
/// queries that V2 supports as of step 4: no-null inputs, no lazy
/// vectors, no shared sub-expressions, no encodings, no special forms
/// requiring conditional row evaluation.
class ExprV2ParityTest : public testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  void assertParity(
      const std::string& exprText,
      const RowVectorPtr& input) {
    auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
    ASSERT_NE(rowType, nullptr);

    auto untyped = parse::DuckSqlExpressionsParser(options_).parseExpr(exprText);
    auto typed = core::Expressions::inferTypes(
        untyped, rowType, execCtx_->pool());

    // V1 path.
    auto v1Set = std::make_shared<ExprSet>(
        std::vector<core::TypedExprPtr>{typed}, execCtx_.get());
    SelectivityVector rows{input->size()};
    EvalCtx v1Ctx{execCtx_.get(), v1Set.get(), input.get()};
    std::vector<VectorPtr> v1Results(1);
    v1Set->eval(rows, v1Ctx, v1Results);

    // V2 path -- adapt the same compiled ExprSet.
    ExprSetV2 v2Set{v1Set};
    EvalCtx v2Ctx{execCtx_.get(), v2Set.sourceSet().get(), input.get()};
    std::vector<VectorPtr> v2Results(1);
    v2Set.eval(rows, v2Ctx, v2Results);

    velox::test::assertEqualVectors(v1Results[0], v2Results[0]);
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  parse::ParseOptions options_;
};

// Bare field reference: a -> a.  Goes through special-form delegation
// (FieldAccess).
TEST_F(ExprV2ParityTest, fieldReference) {
  auto input = makeRowVector(
      {"a"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
  assertParity("a", input);
}

// Two-arg deterministic function on no-null flat inputs.
TEST_F(ExprV2ParityTest, simplePlus) {
  auto input = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50})});
  assertParity("a + b", input);
}

// Nested function calls on no-null flat inputs.
TEST_F(ExprV2ParityTest, nestedArith) {
  auto input = makeRowVector(
      {"a", "b", "c"},
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
       makeFlatVector<int64_t>({100, 200, 300, 400, 500})});
  assertParity("a + b * c", input);
}

// Constant inputs in the expression tree (special-form delegation).
TEST_F(ExprV2ParityTest, constantInExpr) {
  auto input = makeRowVector(
      {"a"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
  assertParity("a + 100::bigint", input);
}

// Boolean function on no-null flat inputs.
TEST_F(ExprV2ParityTest, comparison) {
  auto input = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>({1, 5, 3, 2, 4}),
       makeFlatVector<int64_t>({4, 4, 4, 4, 4})});
  assertParity("a < b", input);
}

// Empty selectivity vector -- emitEmpty path.
TEST_F(ExprV2ParityTest, emptyRows) {
  auto input = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30})});
  auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
  auto untyped = parse::DuckSqlExpressionsParser(options_).parseExpr("a + b");
  auto typed = core::Expressions::inferTypes(
      untyped, rowType, execCtx_->pool());

  auto v1Set = std::make_shared<ExprSet>(
      std::vector<core::TypedExprPtr>{typed}, execCtx_.get());
  SelectivityVector rows{input->size()};
  rows.clearAll();

  EvalCtx v1Ctx{execCtx_.get(), v1Set.get(), input.get()};
  std::vector<VectorPtr> v1Results(1);
  v1Set->eval(rows, v1Ctx, v1Results);

  ExprSetV2 v2Set{v1Set};
  EvalCtx v2Ctx{execCtx_.get(), v2Set.sourceSet().get(), input.get()};
  std::vector<VectorPtr> v2Results(1);
  v2Set.eval(rows, v2Ctx, v2Results);

  // Both should produce a 0-size constant; just check sizes match.
  EXPECT_EQ(v1Results[0]->size(), v2Results[0]->size());
}

} // namespace
} // namespace facebook::velox::exec::test
