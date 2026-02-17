/*
 * Implementation for plan-node-level CUDF evaluation checks.
 */

#include "velox/experimental/cudf/exec/CudfPlanNodeChecker.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/common/memory/Memory.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::cudf_velox {

bool canBeEvaluatedByCudf(
  const core::AggregationNode& aggregationNode,
  core::QueryCtx* queryCtx) {
  const core::PlanNode* sourceNode = aggregationNode.sources().empty()
      ? nullptr
      : aggregationNode.sources()[0].get();

  // Get the aggregation step from the node
  auto step = aggregationNode.step();

  // Check supported aggregation functions using step-aware aggregation registry
  for (const auto& aggregate : aggregationNode.aggregates()) {
    // Use step-aware validation that handles partial/final/intermediate steps
    if (!canAggregationBeEvaluatedByCudf(
            *aggregate.call, step, aggregate.rawInputTypes, queryCtx)) {
      return false;
    }

    if (aggregate.distinct) {
      return false;
    }

    if (aggregate.mask) {
      return false;
    }

    // Check input expressions can be evaluated by CUDF, expand the input
    for (const auto& input : aggregate.call->inputs()) {
      auto expandedInput = expandFieldReference(input, sourceNode);
      std::vector<core::TypedExprPtr> exprs = {expandedInput};
      if (!canBeEvaluatedByCudf(exprs, queryCtx)) {
        return false;
      }
    }
  }

  // Check grouping key expressions
  if (!canGroupingKeysBeEvaluatedByCudf(
          aggregationNode.groupingKeys(), sourceNode, queryCtx)) {
    return false;
  }

  return true;
}

bool canBeEvaluatedByCudf(
    const std::vector<core::TypedExprPtr>& exprs,
    core::QueryCtx* queryCtx) {
  if (exprs.empty()) {
    return true;
  }

  auto precompilePool =
      memory::memoryManager()->addLeafPool("", /*threadSafe*/ false);
  core::ExecCtx precompileCtx(precompilePool.get(), queryCtx);

  bool lazyDereference = false;
  std::vector<core::TypedExprPtr> exprsCopy = exprs;
  std::unique_ptr<exec::ExprSet> exprSet = exec::makeExprSetFromFlag(
      std::move(exprsCopy), &precompileCtx, lazyDereference);

  for (const auto& e : exprSet->exprs()) {
    if (!canBeEvaluatedByCudf(e)) {
      return false;
    }
  }
  return true;
}

} // namespace facebook::velox::cudf_velox
