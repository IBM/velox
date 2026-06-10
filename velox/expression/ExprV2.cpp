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

namespace facebook::velox::exec {

std::shared_ptr<ExprV2> ExprV2::from(const std::shared_ptr<Expr>& expr) {
  VELOX_CHECK_NOT_NULL(expr, "ExprV2::from requires a non-null Expr");

  std::vector<std::shared_ptr<ExprV2>> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& child : expr->inputs()) {
    inputs.push_back(ExprV2::from(child));
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
  node->trackCpuUsage_ = expr->trackCpuUsage();
  node->distinctFields_ = expr->distinctFields();
  node->multiplyReferencedFields_ = expr->multiplyReferencedFields();
  node->sourceExpr_ = expr;

  return node;
}

} // namespace facebook::velox::exec
