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

#include "velox/experimental/cudf-exchange/CudfPartitionedOutput.h"
#include "velox/experimental/cudf-exchange/ExchangeClientFacade.h"
#include "velox/experimental/cudf-exchange/HybridExchange.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/exec/CudfAssignUniqueId.h"
#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/exec/CudfHashJoin.h"
#include "velox/experimental/cudf/exec/CudfLimit.h"
#include "velox/experimental/cudf/exec/CudfLocalPartition.h"
#include "velox/experimental/cudf/exec/CudfOrderBy.h"
#include "velox/experimental/cudf/exec/CudfTopN.h"
#include "velox/experimental/cudf/exec/CudfTopNRowNumber.h"
#include "velox/experimental/cudf/exec/OperatorAdapters.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"

#include "velox/exec/AssignUniqueId.h"
#include "velox/exec/CallbackSink.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashProbe.h"
#include "velox/exec/Limit.h"
#include "velox/exec/LocalPartition.h"
#include "velox/exec/Merge.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/StreamingAggregation.h"
#include "velox/exec/TableScan.h"
#include "velox/exec/Task.h"
#include "velox/exec/TopN.h"
#include "velox/exec/TopNRowNumber.h"
#include "velox/exec/Values.h"

namespace facebook::velox::cudf_velox {

/// OperatorAdapterRegistry Implementation
OperatorAdapterRegistry& OperatorAdapterRegistry::getInstance() {
  static OperatorAdapterRegistry instance;
  return instance;
}

void OperatorAdapterRegistry::registerAdapter(
    std::unique_ptr<OperatorAdapter> adapter) {
  adapters_.push_back(std::move(adapter));
}

const OperatorAdapter* OperatorAdapterRegistry::findAdapter(
    const exec::Operator* op) const {
  for (const auto& adapter : adapters_) {
    if (adapter->canHandle(op)) {
      return adapter.get();
    }
  }
  // Note: It is possible to have priority based adapter search.
  // But this is not implemented because it is not needed for now.
  return nullptr;
}

const std::vector<std::unique_ptr<OperatorAdapter>>&
OperatorAdapterRegistry::getAdapters() const {
  return adapters_;
}

void OperatorAdapterRegistry::clear() {
  adapters_.clear();
}

/// TableScanAdapter - Keeps original operator (produces GPU output)
class TableScanAdapter : public OperatorAdapter {
 public:
  TableScanAdapter() : OperatorAdapter("TableScan") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::TableScan*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    auto tableScanNode =
        std::dynamic_pointer_cast<const core::TableScanNode>(planNode);
    if (!tableScanNode) {
      return false;
    }
    auto const& connector = velox::connector::getConnector(
        tableScanNode->tableHandle()->connectorId());
    auto cudfHiveConnector = std::dynamic_pointer_cast<
        facebook::velox::cudf_velox::connector::hive::CudfHiveConnector>(
        connector);
    return cudfHiveConnector != nullptr;
  }

  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/,
      int32_t /*operatorId*/) const override {
    return {}; // Keep original operator
  }

  bool keepOperator() const override {
    return true;
  }
};

/// FilterProjectAdapter - Replaces with CudfFilterProject
class FilterProjectAdapter : public OperatorAdapter {
 public:
  FilterProjectAdapter() : OperatorAdapter("FilterProject") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::FilterProject*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx) const override {
    auto filterProjectOp = dynamic_cast<const exec::FilterProject*>(op);
    if (!filterProjectOp) {
      return false;
    }

    auto projectPlanNode =
        std::dynamic_pointer_cast<const core::ProjectNode>(planNode);
    auto filterNode = filterProjectOp->filterNode();

    if (projectPlanNode) {
      if (projectPlanNode->sources()[0]->outputType()->size() == 0 ||
          projectPlanNode->outputType()->size() == 0) {
        return false;
      }
    }

    // Check filter separately
    if (filterNode) {
      if (!canBeEvaluatedByCudf(
              {filterNode->filter()}, ctx->task->queryCtx().get())) {
        return false;
      }
    }

    // Check projects separately
    if (projectPlanNode) {
      if (!canBeEvaluatedByCudf(
              projectPlanNode->projections(), ctx->task->queryCtx().get())) {
        return false;
      }
    }
    return true;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto filterProjectOp = dynamic_cast<const exec::FilterProject*>(op);
    auto projectPlanNode =
        std::dynamic_pointer_cast<const core::ProjectNode>(planNode);
    auto filterPlanNode = filterProjectOp->filterNode();

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfFilterProject>(
            operatorId, ctx, filterPlanNode, projectPlanNode));
    return result;
  }
};

/// AggregationAdapter - Replaces with CudfHashAggregation
class AggregationAdapter : public OperatorAdapter {
 public:
  AggregationAdapter() : OperatorAdapter("Aggregation") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::HashAggregation*>(op) != nullptr ||
        dynamic_cast<const exec::StreamingAggregation*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx) const override {
    if (!canHandle(op)) {
      return false;
    }

    auto aggregationPlanNode =
        std::dynamic_pointer_cast<const core::AggregationNode>(planNode);
    if (!aggregationPlanNode) {
      return false;
    }

    if (aggregationPlanNode->sources()[0]->outputType()->size() == 0) {
      // We cannot handle RowVectors with a length but no data.
      // This is the case with count(*) global (without groupby)
      return false;
    }

    return canBeEvaluatedByCudf(
        *aggregationPlanNode, ctx->task->queryCtx().get());
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto aggregationPlanNode =
        std::dynamic_pointer_cast<const core::AggregationNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfHashAggregation>(
            operatorId, ctx, aggregationPlanNode));
    return result;
  }
};

class CudfHashJoinBaseAdapter : public OperatorAdapter {
 public:
  using OperatorAdapter::OperatorAdapter;

  bool canRunOnGPU(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx) const override {
    if (!canHandle(op)) {
      return false;
    }

    auto joinPlanNode =
        std::dynamic_pointer_cast<const core::HashJoinNode>(planNode);
    if (!joinPlanNode) {
      return false;
    }

    if (!CudfHashJoinProbe::isSupportedJoinType(joinPlanNode->joinType())) {
      return false;
    }

    // Disabling null-aware anti join with filter until we implement it right
    if (joinPlanNode->joinType() == core::JoinType::kAnti &&
        joinPlanNode->isNullAware() && joinPlanNode->filter()) {
      return false;
    }

    if (joinPlanNode->filter()) {
      if (!canBeEvaluatedByCudf(
              {joinPlanNode->filter()}, ctx->task->queryCtx().get())) {
        return false;
      }
    }
    return true;
  }
};

/// HashJoinBuildAdapter - Replaces with CudfHashJoinBuild
class HashJoinBuildAdapter : public CudfHashJoinBaseAdapter {
 public:
  HashJoinBuildAdapter() : CudfHashJoinBaseAdapter("HashJoinBuild") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::HashBuild*>(op) != nullptr;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return false;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto joinPlanNode =
        std::dynamic_pointer_cast<const core::HashJoinNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfHashJoinBuild>(operatorId, ctx, joinPlanNode));
    return result;
  }
};

/// HashJoinProbeAdapter - Replaces with CudfHashJoinProbe
class HashJoinProbeAdapter : public CudfHashJoinBaseAdapter {
 public:
  HashJoinProbeAdapter() : CudfHashJoinBaseAdapter("HashJoinProbe") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::HashProbe*>(op) != nullptr;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto joinPlanNode =
        std::dynamic_pointer_cast<const core::HashJoinNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfHashJoinProbe>(operatorId, ctx, joinPlanNode));
    return result;
  }
};

/// OrderByAdapter - Replaces with CudfOrderBy
class OrderByAdapter : public OperatorAdapter {
 public:
  OrderByAdapter() : OperatorAdapter("OrderBy") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::OrderBy*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    return std::dynamic_pointer_cast<const core::OrderByNode>(planNode) !=
        nullptr;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto orderByPlanNode =
        std::dynamic_pointer_cast<const core::OrderByNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfOrderBy>(operatorId, ctx, orderByPlanNode));
    return result;
  }
};

/// TopNAdapter - Replaces with CudfTopN
class TopNAdapter : public OperatorAdapter {
 public:
  TopNAdapter() : OperatorAdapter("TopN") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::TopN*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    return std::dynamic_pointer_cast<const core::TopNNode>(planNode) != nullptr;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto topNPlanNode =
        std::dynamic_pointer_cast<const core::TopNNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(std::make_unique<CudfTopN>(operatorId, ctx, topNPlanNode));
    return result;
  }
};

/// LimitAdapter - Replaces with CudfLimit
class LimitAdapter : public OperatorAdapter {
 public:
  LimitAdapter() : OperatorAdapter("Limit") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::Limit*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    return std::dynamic_pointer_cast<const core::LimitNode>(planNode) !=
        nullptr;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto limitPlanNode =
        std::dynamic_pointer_cast<const core::LimitNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfLimit>(operatorId, ctx, limitPlanNode));
    return result;
  }
};

/// LocalPartitionAdapter - Conditionally replaces with CudfLocalPartition
class LocalPartitionAdapter : public OperatorAdapter {
 public:
  LocalPartitionAdapter() : OperatorAdapter("LocalPartition") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::LocalPartition*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    auto localPartitionPlanNode =
        std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode);
    return canHandle(op) && localPartitionPlanNode &&
        CudfLocalPartition::shouldReplace(localPartitionPlanNode);
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return false;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto localPartitionPlanNode =
        std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfLocalPartition>(
            operatorId, ctx, localPartitionPlanNode));
    return result;
  }

  bool keepOperator() const override {
    return false;
  }
};

/// LocalExchangeAdapter - Keeps original operator
class LocalExchangeAdapter : public OperatorAdapter {
 public:
  LocalExchangeAdapter() : OperatorAdapter("LocalExchange") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::LocalExchange*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/) const override {
    return true;
  }

  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/,
      int32_t /*operatorId*/) const override {
    return {}; // Keep original operator
  }

  bool keepOperator() const override {
    return true;
  }
};

/// AssignUniqueIdAdapter - Replaces with CudfAssignUniqueId
class AssignUniqueIdAdapter : public OperatorAdapter {
 public:
  AssignUniqueIdAdapter() : OperatorAdapter("AssignUniqueId") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::AssignUniqueId*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    return std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(
               planNode) != nullptr;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto assignUniqueIdPlanNode =
        std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    result.push_back(
        std::make_unique<CudfAssignUniqueId>(
            operatorId,
            ctx,
            assignUniqueIdPlanNode,
            assignUniqueIdPlanNode->taskUniqueId(),
            assignUniqueIdPlanNode->uniqueIdCounter()));
    return result;
  }
};

/// ValuesAdapter - Keeps original operator
class ValuesAdapter : public OperatorAdapter {
 public:
  ValuesAdapter() : OperatorAdapter("Values") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::Values*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/) const override {
    return false;
  }

  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return false;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/,
      int32_t /*operatorId*/) const override {
    return {}; // Keep original operator
  }

  bool keepOperator() const override {
    return true;
  }
};

/// CallbackSinkAdapter - Keeps original operator
class CallbackSinkAdapter : public OperatorAdapter {
 public:
  CallbackSinkAdapter() : OperatorAdapter("CallbackSink") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::CallbackSink*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/) const override {
    return false;
  }

  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return false;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/,
      int32_t /*operatorId*/) const override {
    return {}; // Keep original operator
  }

  bool keepOperator() const override {
    return true;
  }
};

/// TopNRowNumberAdapter - Replaces with CudfTopNRowNumber
class TopNRowNumberAdapter : public OperatorAdapter {
 public:
  TopNRowNumberAdapter() : OperatorAdapter("TopNRowNumber") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::TopNRowNumber*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    return std::dynamic_pointer_cast<const core::TopNRowNumberNode>(planNode) !=
        nullptr;
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto topNRowNumberPlanNode =
        std::dynamic_pointer_cast<const core::TopNRowNumberNode>(planNode);

    std::vector<std::unique_ptr<exec::Operator>> result;
    VELOX_CHECK(
        CudfTopNRowNumber::shouldReplace(topNRowNumberPlanNode),
        "CudfTopNRowNumber only supports limit=1 with row_number function");
    result.push_back(
        std::make_unique<CudfTopNRowNumber>(
            operatorId, ctx, topNRowNumberPlanNode));
    return result;
  }
};

// Cudf Exchange Facade
// Helpers for the CudfExchange operator replacement logic
struct TaskPipelineKey {
  std::string taskId;
  int pipelineId;

  TaskPipelineKey(const std::string& tid, int pid)
      : taskId(tid), pipelineId(pid) {}

  // need equality operator for unordered map.
  bool operator==(const TaskPipelineKey& other) const {
    return taskId == other.taskId && pipelineId == other.pipelineId;
  }

  // Need a hash functor for the unordered map.
  struct Hash {
    std::size_t operator()(const TaskPipelineKey& key) const {
      std::hash<std::string> hasher;
      std::size_t h1 = hasher(key.taskId);
      std::size_t h2 = std::hash<int>{}(key.pipelineId);
      return h1 ^ (h2 << 1); // simple combination of the two hash functions.
    }
  };
};

// Map to store ExchangeClientFacade instances by task and pipeline.
// Declared in ToCudf.cpp to ensure a single instance across all translation
// units.
using ExchangeClientFacadeMap = std::unordered_map<
    TaskPipelineKey,
    std::shared_ptr<cudf_exchange::ExchangeClientFacade>,
    TaskPipelineKey::Hash>;

ExchangeClientFacadeMap& getExchangeClientFacadeMap();

// Single instance of the map to store ExchangeClientFacade instances.
// Using a function with a static local ensures thread-safe initialization
// and a single instance across all translation units.
ExchangeClientFacadeMap& getExchangeClientFacadeMap() {
  static ExchangeClientFacadeMap instance;
  return instance;
}

// Mutex to protect concurrent access to the ExchangeClientFacadeMap.
std::mutex& getExchangeClientFacadeMapMutex() {
  static std::mutex instance;
  return instance;
}

/// ExchangeAdapter - Replaces with CudfExchange if enabled
class ExchangeAdapter : public OperatorAdapter {
 public:
  ExchangeAdapter() : OperatorAdapter("Exchange") {}

  bool canHandle(const exec::Operator* op) const override {
    return CudfConfig::getInstance().exchange &&
        dynamic_cast<const exec::Exchange*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/) const override {
    return CudfConfig::getInstance().exchange;
  }

  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    std::vector<std::unique_ptr<exec::Operator>> result;
    auto exchangeOp = (exec::Exchange*)dynamic_cast<const exec::Exchange*>(op);
    // Get or create the ExchangeClientFacade, using parameters from the
    // Velox exchange client.
    auto key = TaskPipelineKey(op->taskId(), ctx->pipelineId);
    std::shared_ptr<facebook::velox::cudf_exchange::ExchangeClientFacade>
        client = nullptr;
    {
      std::lock_guard<std::mutex> lock(getExchangeClientFacadeMapMutex());
      auto& facadeMap = getExchangeClientFacadeMap();
      auto clientIter = facadeMap.find(key);
      if (clientIter == facadeMap.end()) {
        // The following std::move transfers the ownership of the
        // HttpExchangeClient to the facade, preventing that it is closed
        // when the ExchangeOperator is destructed after being replace by
        // the HybridExchange.
        auto veloxExchangeClient = exchangeOp->releaseExchangeClient();
        VELOX_CHECK_NOT_NULL(
            veloxExchangeClient, "Velox exchange client can't be null.");
        // create new cudfExchangeClient
        auto cudfClient = std::make_shared<
            facebook::velox::cudf_exchange::CudfExchangeClient>(
            op->taskId(),
            veloxExchangeClient->getDestination(),
            veloxExchangeClient->getNumberOfConsumers());
        client = std::make_shared<
            facebook::velox::cudf_exchange::ExchangeClientFacade>(
            op->taskId(),
            ctx->pipelineId,
            std::move(cudfClient),
            std::move(veloxExchangeClient));
        facadeMap.emplace(key, client);
      } else {
        client = clientIter->second;
        // prevent closing of HttpExchangeClient when ExchangeOperator is
        // destructed after being replaced by the ExchangeClientFacade
        exchangeOp->resetExchangeClient();
      }
    }
    result.push_back(
        std::make_unique<facebook::velox::cudf_exchange::HybridExchange>(
            operatorId, ctx, planNode, client));
    return result;
  }

  bool keepOperator() const override {
    return !CudfConfig::getInstance().exchange;
  }
};

/// MergeExchangeAdapter - Replaces with CudfOrderBy AND CudfExchange if enabled
class MergeExchangeAdapter : public OperatorAdapter {
 public:
  MergeExchangeAdapter() : OperatorAdapter("MergeExchange") {}

  bool canHandle(const exec::Operator* op) const override {
    return CudfConfig::getInstance().exchange &&
        dynamic_cast<const exec::MergeExchange*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/) const override {
    return CudfConfig::getInstance().exchange;
  }

  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    std::vector<std::unique_ptr<exec::Operator>> result;
    // create a HybridExchange operator for the merge exchange.
    // Pass a nullptr to force the HybridExchange op to create its
    // own, private CudfExchangeClient.
    result.push_back(
        std::make_unique<facebook::velox::cudf_exchange::HybridExchange>(
            operatorId, ctx, planNode, nullptr));
    // Add an order-by node. SortingKeys and SortOrders will be taken from
    // the MergeExchangeNode.
    result.push_back(std::make_unique<CudfOrderBy>(operatorId, ctx, planNode));
    return result;
  }

  bool keepOperator() const override {
    return !CudfConfig::getInstance().exchange;
  }
};

/// PartitionedOutputAdapter - Replaces with CudfExchange if enabled
class PartitionedOutputAdapter : public OperatorAdapter {
 public:
  PartitionedOutputAdapter() : OperatorAdapter("PartitionedOutput") {}

  bool canHandle(const exec::Operator* op) const override {
    return CudfConfig::getInstance().exchange &&
        dynamic_cast<const exec::PartitionedOutput*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    auto poNode =
        std::dynamic_pointer_cast<const core::PartitionedOutputNode>(planNode);
    return CudfConfig::getInstance().exchange && !poNode->isRootFragment();
  }

  bool acceptsGpuInput() const override {
    return true;
  }

  bool producesGpuOutput() const override {
    return false;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* op,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    std::vector<std::unique_ptr<exec::Operator>> result;
    auto partitionOp = dynamic_cast<const exec::PartitionedOutput*>(op);
    auto poNode =
        std::dynamic_pointer_cast<const core::PartitionedOutputNode>(planNode);
    result.push_back(
        std::make_unique<facebook::velox::cudf_exchange::CudfPartitionedOutput>(
            operatorId, ctx, poNode, partitionOp->getEagerFlush()));

    return result;
  }

  bool keepOperator() const override {
    return !CudfConfig::getInstance().exchange;
  }
};

/// Registration Function
void registerAllOperatorAdapters() {
  auto& registry = OperatorAdapterRegistry::getInstance();

  // Clear any existing adapters
  registry.clear();

  // Register all adapters
  registry.registerAdapter(std::make_unique<TableScanAdapter>());
  registry.registerAdapter(std::make_unique<FilterProjectAdapter>());
  registry.registerAdapter(std::make_unique<AggregationAdapter>());
  registry.registerAdapter(std::make_unique<HashJoinBuildAdapter>());
  registry.registerAdapter(std::make_unique<HashJoinProbeAdapter>());
  registry.registerAdapter(std::make_unique<OrderByAdapter>());
  registry.registerAdapter(std::make_unique<TopNAdapter>());
  registry.registerAdapter(std::make_unique<LimitAdapter>());
  registry.registerAdapter(std::make_unique<LocalPartitionAdapter>());
  registry.registerAdapter(std::make_unique<LocalExchangeAdapter>());
  registry.registerAdapter(std::make_unique<AssignUniqueIdAdapter>());
  registry.registerAdapter(std::make_unique<ValuesAdapter>());
  registry.registerAdapter(std::make_unique<CallbackSinkAdapter>());
  registry.registerAdapter(std::make_unique<TopNRowNumberAdapter>());
  registry.registerAdapter(std::make_unique<ExchangeAdapter>());
  registry.registerAdapter(std::make_unique<MergeExchangeAdapter>());
  registry.registerAdapter(std::make_unique<PartitionedOutputAdapter>());
}

} // namespace facebook::velox::cudf_velox
