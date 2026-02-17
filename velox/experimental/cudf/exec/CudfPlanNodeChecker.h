/*
 * Helper to check whether plan nodes can be evaluated by CUDF.
 */
#pragma once

#include "velox/core/PlanNode.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"

namespace facebook::velox::cudf_velox {

bool canBeEvaluatedByCudf(
	const core::AggregationNode& aggregationNode,
	core::QueryCtx* queryCtx);

// Operator/plan-node level predicate: can a group of expressions
// (e.g. filter + projections) be evaluated by the CUDF operator.
bool canBeEvaluatedByCudf(
	const std::vector<core::TypedExprPtr>& exprs,
	core::QueryCtx* queryCtx);

} // namespace facebook::velox::cudf_velox
