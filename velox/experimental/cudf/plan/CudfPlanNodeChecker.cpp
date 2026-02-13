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
/*
 * Implementation for plan-node-level CUDF evaluation checks.
 */

#include "velox/experimental/cudf/plan/CudfPlanNodeChecker.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/experimental/cudf/exec/CudfHashJoin.h"
#include "velox/common/memory/Memory.h"
#include "velox/expression/Expr.h"
#include "velox/connectors/Connector.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/plan/CudfExpressionChecker.h"

namespace facebook::velox::cudf_velox {

bool isTableScanNodeSupported(const core::TableScanNode* tableScanNode) {
  if (!tableScanNode) {
    return false;
  }

  auto const& connector = velox::connector::getConnector(
      tableScanNode->tableHandle()->connectorId());
  auto cudfHiveConnector = std::dynamic_pointer_cast<
      facebook::velox::cudf_velox::connector::hive::CudfHiveConnector>(
      connector);
  if (!cudfHiveConnector) {
    return false;
  }
  // TODO (dm): we need to ask CudfHiveConnector whether this table handle is
  // supported by it. It may choose to produce a HiveDatasource.
  return true;
}

bool isFilterNodeSupported(const core::FilterNode* filterNode) {
  if (!filterNode) {
    return true;  // No filter, so supported
  }

  return canBeEvaluatedByCudf(
      std::vector<velox::core::TypedExprPtr>{filterNode->filter()});
}

bool isProjectNodeSupported(const core::ProjectNode* projectNode) {
  if (!projectNode) {
    return true;  // No project, so supported
  }

  // Check that source and output types are not empty
  if (projectNode->sources()[0]->outputType()->size() == 0 ||
      projectNode->outputType()->size() == 0) {
    return false;
  }

  return canBeEvaluatedByCudf(projectNode->projections());
}

bool isAggregationNodeSupported(
    const core::AggregationNode* aggregationNode) {
  if (!aggregationNode) {
    return false;
  }

  if (aggregationNode->sources()[0]->outputType()->size() == 0) {
    // We cannot handle RowVectors with a length but no data.
    // This is the case with count(*) global (without groupby)
    return false;
  }

  const core::PlanNode* sourceNode = aggregationNode->sources().empty()
      ? nullptr
      : aggregationNode->sources()[0].get();

  // Get the aggregation step from the node
  auto step = aggregationNode->step();

  // Check supported aggregation functions using step-aware aggregation registry
  // Create a minimal ExecCtx without queryCtx
  auto dummyQueryCtx = core::QueryCtx::create();
  for (const auto& aggregate : aggregationNode->aggregates()) {
    // Use step-aware validation that handles partial/final/intermediate steps
    if (!canAggregationBeEvaluatedByCudf(
            *aggregate.call, step, aggregate.rawInputTypes, dummyQueryCtx.get())) {
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
      if (!canBeEvaluatedByCudf(exprs)) {
        return false;
      }
    }
  }

  // Check grouping key expressions
  if (!canGroupingKeysBeEvaluatedByCudf(
          aggregationNode->groupingKeys(), sourceNode, dummyQueryCtx.get())) {
    return false;
  }

  return true;
}

bool isHashJoinNodeSupported(
    const core::HashJoinNode* joinNode) {
  if (!joinNode) {
    return false;
  }

  if (!CudfHashJoinProbe::isSupportedJoinType(joinNode->joinType())) {
    return false;
  }

  // disabling null-aware anti join with filter until we implement it right
  if (joinNode->joinType() == core::JoinType::kAnti and
      joinNode->isNullAware() and joinNode->filter()) {
    return false;
  }

  if (joinNode->filter()) {
    if (!canBeEvaluatedByCudf(
            std::vector<velox::core::TypedExprPtr>{joinNode->filter()})) {
      return false;
    }
  }
  return true;
}

} // namespace facebook::velox::cudf_velox
