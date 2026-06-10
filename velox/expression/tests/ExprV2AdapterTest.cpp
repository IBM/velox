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

#include "velox/expression/ExprV2.h"

#include <gtest/gtest.h>

#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprRuntimeState.h"
#include "velox/expression/ExprSetV2.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
namespace {

class ExprV2AdapterTest : public testing::Test,
                          public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::DuckSqlExpressionsParser(options_).parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  std::shared_ptr<ExprSet> compile(
      const std::string& text,
      const RowTypePtr& rowType) {
    return std::make_shared<ExprSet>(
        std::vector<core::TypedExprPtr>{parseExpression(text, rowType)},
        execCtx_.get());
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  parse::ParseOptions options_;
};

// Adapter must produce a tree of the same shape as the source Expr,
// with every node populated and sourceExpr pointing back to the source.
TEST_F(ExprV2AdapterTest, functionCallShape) {
  auto rowType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  auto exprSet = compile("a + b * 2::bigint", rowType);
  const auto& root = exprSet->exprs().front();

  auto v2 = ExprV2::from(root);
  ASSERT_NE(v2, nullptr);

  // Root: plus(a, multiply(b, 2)).
  EXPECT_EQ(v2->type()->toString(), root->type()->toString());
  EXPECT_EQ(v2->name(), root->name());
  EXPECT_EQ(v2->inputs().size(), root->inputs().size());
  EXPECT_FALSE(v2->isSpecialForm());
  EXPECT_EQ(v2->deterministic(), root->isDeterministic());
  EXPECT_EQ(v2->propagatesNulls(), root->propagatesNulls());
  EXPECT_EQ(
      v2->supportsFlatNoNullsFastPath(), root->supportsFlatNoNullsFastPath());
  EXPECT_EQ(v2->hasConditionals(), root->hasConditionals());
  EXPECT_EQ(v2->sourceExpr().get(), root.get());

  // Function pointer and metadata round-trip.
  EXPECT_EQ(v2->vectorFunction().get(), root->vectorFunction().get());
  EXPECT_EQ(
      v2->metadata().defaultNullBehavior,
      root->vectorFunctionMetadata().defaultNullBehavior);

  // Recurse into children and verify the same.
  for (size_t i = 0; i < v2->inputs().size(); ++i) {
    const auto& childV2 = v2->inputs()[i];
    const auto& childV1 = root->inputs()[i];
    EXPECT_EQ(childV2->sourceExpr().get(), childV1.get());
    EXPECT_EQ(childV2->inputs().size(), childV1->inputs().size());
    EXPECT_EQ(childV2->name(), childV1->name());
  }
}

// Adapter must tag special-form nodes with their SpecialFormKind.
TEST_F(ExprV2AdapterTest, specialFormTag) {
  auto rowType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  // Switch is a special form.
  auto exprSet = compile("case when a > 0 then b else 0::bigint end", rowType);
  const auto& root = exprSet->exprs().front();

  auto v2 = ExprV2::from(root);

  EXPECT_TRUE(v2->isSpecialForm());
  EXPECT_EQ(v2->specialFormKind(), root->specialFormKind());

  // Special-form nodes still retain sourceExpr for delegation.
  EXPECT_EQ(v2->sourceExpr().get(), root.get());
}

// distinctFields and multiplyReferencedFields raw pointers must remain
// valid after adaptation -- they still point into the V1 tree, which
// is kept alive via sourceExpr.
TEST_F(ExprV2AdapterTest, fieldReferencePointersPreserved) {
  auto rowType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  auto exprSet = compile("a + a + b", rowType);
  const auto& root = exprSet->exprs().front();

  auto v2 = ExprV2::from(root);

  EXPECT_EQ(v2->distinctFields().size(), root->distinctFields().size());
  for (size_t i = 0; i < v2->distinctFields().size(); ++i) {
    // Raw pointers should be bit-identical to the V1 set.
    EXPECT_EQ(v2->distinctFields()[i], root->distinctFields()[i]);
  }
  EXPECT_EQ(
      v2->multiplyReferencedFields().size(),
      root->multiplyReferencedFields().size());
}

// ExprSetV2 should adapt every root from the source ExprSet and build a
// runtime-state tree covering all unique nodes.
TEST_F(ExprV2AdapterTest, exprSetV2Construction) {
  auto rowType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  parse::ParseOptions options;
  std::vector<core::TypedExprPtr> typed{
      parseExpression("a + b", rowType),
      parseExpression("a * b", rowType),
  };
  auto exprSet = std::make_shared<ExprSet>(std::move(typed), execCtx_.get());

  ExprSetV2 v2{exprSet};

  ASSERT_EQ(v2.exprs().size(), 2);
  EXPECT_EQ(v2.exprs()[0]->sourceExpr().get(), exprSet->exprs()[0].get());
  EXPECT_EQ(v2.exprs()[1]->sourceExpr().get(), exprSet->exprs()[1].get());

  // Runtime-state tree should cover at least the 2 roots + their unique
  // descendants.  Each root is binary so total >= 2 (roots) + 4
  // (FieldReferences) = 6.  Use >= to keep the test robust to minor
  // tree-shape variation.
  EXPECT_GE(v2.runtimeStates().size(), 6u);

  // at() returns distinct state instances per node.
  auto& s0 = v2.runtimeStates().at(*v2.exprs()[0]);
  auto& s1 = v2.runtimeStates().at(*v2.exprs()[1]);
  EXPECT_NE(&s0, &s1);
}

} // namespace
} // namespace facebook::velox::exec::test
