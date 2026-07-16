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

#pragma once

#include <memory>
#include <vector>

#include "velox/expression/Expr.h"
#include "velox/expression/ExprEvaluatorV2.h"
#include "velox/expression/ExprRuntimeState.h"
#include "velox/expression/ExprV2.h"

namespace facebook::velox::exec {

/// Routes expression evaluation through the V2 evaluator.
///
/// ExprSetV2 IS-A ExprSet (mirroring ExprSetSimplified's relationship
/// to ExprSet).  Construction goes through the same compiler pipeline
/// as the base class — ExprCompiler builds the V1 Expr tree first, and
/// the parallel ExprV2 mirror tree is built on top in this
/// constructor.  Callers hold ExprSet pointers; the virtual eval()
/// dispatches to this override when the active ExprSet is a V2 one.
///
/// Constructed by makeExprSetFromFlag (Expr.cpp) when
/// QueryConfig::exprEvalV2() is true.  Existing operator callers
/// (FilterProject, ParallelProject) pick it up automatically through
/// that factory.
class ExprSetV2 : public ExprSet {
 public:
  /// Constructs the base ExprSet (V1 Expr tree via ExprCompiler), then
  /// adapts each root into the parallel ExprV2 mirror tree and builds
  /// the per-node ExprRuntimeStateTree.
  ExprSetV2(
      const std::vector<core::TypedExprPtr>& source,
      core::ExecCtx* execCtx,
      bool enableConstantFolding = true,
      bool lazyDereference = false);

  ~ExprSetV2() override = default;

  // Un-hide the base-class eval overloads (notably the 3-argument
  // convenience wrapper) that the eval override below would otherwise
  // hide via name lookup.
  using ExprSet::eval;

  /// Drives the V2 pipeline.  Mirrors ExprSet::eval's setup work
  /// (clearSharedSubexprs, initializeAdaptiveCpuSampling, lazy field
  /// pre-loading) and then iterates V2 roots through ExprEvaluatorV2
  /// instead of calling Expr::eval.
  void eval(
      int32_t begin,
      int32_t end,
      bool initialize,
      const SelectivityVector& rows,
      EvalCtx& ctx,
      std::vector<VectorPtr>& result) override;

  /// V2 mirror tree, parallel to the base class's exprs() but holding
  /// ExprV2 nodes.  Each ExprV2 retains a shared_ptr to its source
  /// Expr (which lives in the base class's exprs_).
  const std::vector<std::shared_ptr<ExprV2>>& exprsV2() const {
    return roots_;
  }

  ExprRuntimeStateTree& runtimeStates() {
    return *runtimeStates_;
  }

 private:
  std::vector<std::shared_ptr<ExprV2>> roots_;
  std::unique_ptr<ExprRuntimeStateTree> runtimeStates_;
  ExprEvaluatorV2 evaluator_;
};

} // namespace facebook::velox::exec
