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

#include "velox/expression/ExprSetV2.h"

#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/exec/trace/TraceCtx.h"
#include "velox/exec/trace/TraceWriter.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

namespace {

// Walks the V2 tree once, installing per-Expr output tracers on
// nodes whose name is in the trace set.  Mirrors Expr::maybeSetupTracer
// (Expr.cpp:2081).  Uses a visited set to avoid redundant work on
// shared CSE nodes, and an instance counter so multiple Expressions
// sharing the same name get distinct tracer indices.
void maybeSetupTracerRecursive(
    ExprV2& expr,
    ExprRuntimeStateTree& runtimeStates,
    const Operator& op,
    const trace::TraceCtx& traceCtx,
    std::unordered_set<const ExprV2*>& visited,
    std::unordered_map<std::string, int>& instanceCounts) {
  if (!visited.insert(&expr).second) {
    return;
  }
  if (traceCtx.shouldTraceExpr(expr.name())) {
    const int index = instanceCounts[expr.name()]++;
    try {
      runtimeStates.at(expr).outputTracer =
          traceCtx.createExprOutputTracer(op, expr.name(), index);
      if (expr.vectorFunction()) {
        traceCtx.maybeActivateIntraExprTracing(
            op, expr.name(), *expr.vectorFunction());
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to set up expression tracer: " << e.what();
    }
  }
  for (const auto& input : expr.inputs()) {
    maybeSetupTracerRecursive(
        *input, runtimeStates, op, traceCtx, visited, instanceCounts);
  }
}

void finishTracerRecursive(
    ExprV2& expr,
    ExprRuntimeStateTree& runtimeStates,
    std::unordered_set<const ExprV2*>& visited) {
  if (!visited.insert(&expr).second) {
    return;
  }
  auto& state = runtimeStates.at(expr);
  if (state.outputTracer) {
    try {
      state.outputTracer->finish();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to finish expression output tracer: " << e.what();
    }
  }
  for (const auto& input : expr.inputs()) {
    finishTracerRecursive(*input, runtimeStates, visited);
  }
}

} // namespace

ExprSetV2::ExprSetV2(std::shared_ptr<ExprSet> source)
    : sourceSet_{std::move(source)} {
  VELOX_CHECK_NOT_NULL(sourceSet_, "ExprSetV2 requires a non-null ExprSet");

  roots_.reserve(sourceSet_->exprs().size());
  for (const auto& root : sourceSet_->exprs()) {
    roots_.push_back(ExprV2::from(root));
  }

  runtimeStates_ = std::make_unique<ExprRuntimeStateTree>(roots_);
}

void ExprSetV2::eval(
    const SelectivityVector& rows,
    EvalCtx& ctx,
    std::vector<VectorPtr>& results) {
  VELOX_CHECK_EQ(
      results.size(),
      roots_.size(),
      "results vector must be sized to the number of expression roots");

  for (size_t i = 0; i < roots_.size(); ++i) {
    EvalFrame frame{*roots_[i], *runtimeStates_, ctx, rows, results[i]};
    evaluator_.evaluate(frame, this);
  }
}

void ExprSetV2::maybeSetupTracers(
    const Operator& op,
    const trace::TraceCtx& traceCtx) {
  tracingEnabled_ = true;
  std::unordered_set<const ExprV2*> visited;
  std::unordered_map<std::string, int> instanceCounts;
  for (auto& root : roots_) {
    maybeSetupTracerRecursive(
        *root, *runtimeStates_, op, traceCtx, visited, instanceCounts);
  }
}

void ExprSetV2::finishTracers() {
  if (!tracingEnabled_) {
    return;
  }
  std::unordered_set<const ExprV2*> visited;
  for (auto& root : roots_) {
    finishTracerRecursive(*root, *runtimeStates_, visited);
  }
}

} // namespace facebook::velox::exec
