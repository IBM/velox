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

#include "velox/expression/ExprSetV2.h"

#include "velox/common/base/Exceptions.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {

ExprSetV2::ExprSetV2(
    const std::vector<core::TypedExprPtr>& source,
    core::ExecCtx* execCtx,
    bool enableConstantFolding,
    bool lazyDereference)
    : ExprSet(source, execCtx, enableConstantFolding, lazyDereference) {
  // Base class constructor has populated exprs_ via ExprCompiler.
  // Walk it once to build the V2 mirror tree.
  roots_.reserve(exprs().size());
  for (const auto& root : exprs()) {
    roots_.push_back(ExprV2::from(root));
  }
  runtimeStates_ = std::make_unique<ExprRuntimeStateTree>(roots_);
}

void ExprSetV2::eval(
    int32_t begin,
    int32_t end,
    bool initialize,
    const SelectivityVector& rows,
    EvalCtx& context,
    std::vector<VectorPtr>& result) {
  VELOX_CHECK_EQ(lazyDereference(), context.lazyDereference());
  result.resize(exprs().size());

  // Match ExprSet::eval's setup work (Expr.cpp:2305-2334).  These are
  // protected helpers on the base class.
  if (initialize) {
    clearSharedSubexprs();
  }
  if (adaptiveCpuSampling_) {
    initializeAdaptiveCpuSampling(context);
  }
  if (!lazyDereference()) {
    for (const auto& field : multiplyReferencedFields_) {
      context.ensureFieldLoaded(field->index(context), rows);
    }
  }

  // V2 root iteration.  Equivalent to V1's exprs_[i]->eval(...) loop,
  // but drives the V2 evaluator against each V2 root.
  for (int32_t i = begin; i < end; ++i) {
    EvalFrame frame{*roots_[i], *runtimeStates_, context, rows, result[i]};
    evaluator_.evaluate(frame, this);
  }
}

} // namespace facebook::velox::exec
