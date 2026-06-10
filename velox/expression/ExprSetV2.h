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

#include "velox/expression/ExprEvaluatorV2.h"
#include "velox/expression/ExprRuntimeState.h"
#include "velox/expression/ExprV2.h"

namespace facebook::velox::exec::trace {
class TraceCtx;
} // namespace facebook::velox::exec::trace

namespace facebook::velox::exec {

class ExprSet;
class Operator;

/// Owner of a tree of immutable ExprV2 nodes plus the parallel runtime
/// state tree and the stateless evaluator.  Mirrors ExprSet's public
/// shape but routes evaluation through the V2 pipeline.
///
/// During the migration period, ExprSetV2 is constructed from an
/// existing ExprSet (which already holds compiled Expr trees).  This
/// avoids duplicating any compiler, parser, or registry logic.
class ExprSetV2 {
 public:
  /// Adapts an existing compiled ExprSet to V2 by walking each root
  /// Expr and producing a parallel ExprV2 tree.  The source ExprSet is
  /// retained for lifetime of FieldReference pointers and for
  /// delegated special-form evaluation.
  explicit ExprSetV2(std::shared_ptr<ExprSet> source);

  /// Evaluates all roots for 'rows' and writes outputs into 'results'.
  void eval(
      const SelectivityVector& rows,
      EvalCtx& ctx,
      std::vector<VectorPtr>& results);

  const std::vector<std::shared_ptr<ExprV2>>& exprs() const {
    return roots_;
  }

  ExprRuntimeStateTree& runtimeStates() {
    return *runtimeStates_;
  }

  /// The V1 ExprSet this V2 set was adapted from.  During the migration
  /// period, callers pass this to EvalCtx so it can route exception
  /// context, memo updates, and tracer hooks through V1's bookkeeping.
  const std::shared_ptr<ExprSet>& sourceSet() const {
    return sourceSet_;
  }

  /// Mirrors ExprSet::maybeSetupTracers (Expr.cpp:2070).  Walks the V2
  /// tree once and installs per-Expr output tracers on every node
  /// whose name is in the operator's trace set.  Tracer state lives
  /// on ExprRuntimeState::outputTracer.
  void maybeSetupTracers(
      const Operator& op,
      const trace::TraceCtx& traceCtx);

  /// Mirrors ExprSet::finishTracers (Expr.cpp:2105).  Flushes and
  /// closes every output tracer set up by maybeSetupTracers.  Safe to
  /// call when tracing was never enabled (no-op in that case).
  void finishTracers();

 private:
  std::shared_ptr<ExprSet> sourceSet_;
  std::vector<std::shared_ptr<ExprV2>> roots_;
  std::unique_ptr<ExprRuntimeStateTree> runtimeStates_;
  ExprEvaluatorV2 evaluator_;
  bool tracingEnabled_{false};
};

} // namespace facebook::velox::exec
