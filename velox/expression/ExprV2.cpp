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

#include "velox/common/base/Exceptions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/CastExprV2.h"
#include "velox/expression/ExprConstants.h"

namespace facebook::velox::exec {

namespace {

// Constructs a CastExprV2 from a V1 CastExpr.  The V1 CastExpr stays
// alive elsewhere (in ExprSet's exprs_), so raw FieldReference*
// pointers held by the V2 mirror remain valid for the lifetime of
// the V2 tree.  Hooks are produced by the V2-registered factory,
// independently of whatever hooks V1's CastExpr was built with.
std::shared_ptr<CastExprV2> substituteCastExprV2(
    const std::shared_ptr<Expr>& expr,
    core::ExecCtx& execCtx) {
  VELOX_DCHECK(expr->isSpecialForm());
  VELOX_DCHECK(expr->specialFormKind() == SpecialFormKind::kCast);
  const bool isTryCast = (expr->name() == expression::kTryCast);
  // The V1 CastExpr has exactly one input (the expression being
  // cast).  CastExprV2 shares ownership via shared_ptr — V1's tree
  // still holds the same child.
  auto childExpr = expr->inputs()[0];
  auto hooks = CastExprV2::hooksFactory()(
      execCtx.queryCtx()->queryConfig(), isTryCast);
  return std::make_shared<CastExprV2>(
      expr->type(),
      std::move(childExpr),
      expr->trackCpuUsage(),
      isTryCast,
      std::move(hooks));
}

} // namespace

// TODO: this adapter is transitional.  ExprV2::from exists only
// because the compiler produces V1 Expr trees and the V2 evaluator
// wants ExprV2 nodes.  Today's ExprV2 is a thin wrapper that carries
// no per-node data the V1 Expr doesn't already expose; the actual V2
// win (immutable nodes, mutable state in a side ExprRuntimeStateTree)
// doesn't require a separate node type.
//
// When V1 is deleted, one of two things happens:
//   1. ExprV2 is renamed to Expr and this adapter disappears with the
//      old Expr class.
//   2. ExprV2 is dropped entirely; the V2 evaluator switches to
//      Expr& + ExprRuntimeStateTree& directly, and this adapter
//      simply goes away.
// Either path collapses this file to nothing.
std::shared_ptr<ExprV2> ExprV2::from(
    const std::shared_ptr<Expr>& expr,
    core::ExecCtx& execCtx) {
  VELOX_CHECK_NOT_NULL(expr, "ExprV2::from requires a non-null Expr");

  std::vector<std::shared_ptr<ExprV2>> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& child : expr->inputs()) {
    inputs.push_back(ExprV2::from(child, execCtx));
  }

  auto node = std::shared_ptr<ExprV2>(new ExprV2());
  node->type_ = expr->type();
  node->name_ = expr->name();
  node->inputs_ = std::move(inputs);
  node->vectorFunction_ = expr->vectorFunction();
  node->metadata_ = expr->vectorFunctionMetadata();
  node->listeners_ = expr->listeners();
  if (expr->isSpecialForm()) {
    node->specialFormKind_ = expr->specialFormKind();
  }
  node->deterministic_ = expr->isDeterministic();
  node->propagatesNulls_ = expr->propagatesNulls();
  node->supportsFlatNoNullsFastPath_ = expr->supportsFlatNoNullsFastPath();
  node->hasConditionals_ = expr->hasConditionals();
  node->skipFieldDependentOptimizations_ =
      expr->skipFieldDependentOptimizations();
  node->isMultiplyReferenced_ = expr->isMultiplyReferenced();
  node->trackCpuUsage_ = expr->trackCpuUsage();
  node->distinctFields_ = expr->distinctFields();
  node->multiplyReferencedFields_ = expr->multiplyReferencedFields();

  // For kCast nodes, substitute a freshly-built CastExprV2 as the
  // dispatch target so V2 cast evaluation routes through CastExprV2's
  // evalSpecialForm instead of V1's CastExpr.  The V1 CastExpr still
  // exists in V1's ExprSet::exprs_ tree (we don't touch it); only
  // sourceExpr_ in the V2 mirror changes.
  if (expr->isSpecialForm() &&
      expr->specialFormKind() == SpecialFormKind::kCast) {
    node->sourceExpr_ = substituteCastExprV2(expr, execCtx);
  } else {
    node->sourceExpr_ = expr;
  }

  return node;
}

} // namespace facebook::velox::exec
