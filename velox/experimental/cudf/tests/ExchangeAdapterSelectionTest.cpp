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

#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/OperatorAdapters.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/core/ExchangeTransportType.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Merge.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::test {
namespace {

using core::ExchangeTransportType;

/// Verifies that exchange operator adapters make correct keep/replace decisions
/// based on CudfConfig::exchange and the transport type settings on QueryCtx.
class ExchangeAdapterSelectionTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    cudf_velox::registerCudf();
    savedExchange_ = cudf_velox::CudfConfig::getInstance().exchange;
    cudf_velox::CudfConfig::getInstance().exchange = true;
  }

  void TearDown() override {
    cudf_velox::CudfConfig::getInstance().exchange = savedExchange_;
    cudf_velox::unregisterCudf();
    OperatorTestBase::TearDown();
  }

  /// Creates a QueryCtx with per-node transport type settings.
  std::shared_ptr<core::QueryCtx> makeQueryCtx(
      const std::string& inputNodeId = "",
      ExchangeTransportType inputTransport = ExchangeTransportType::kHttp,
      const std::string& outputNodeId = "",
      ExchangeTransportType outputTransport = ExchangeTransportType::kHttp) {
    auto builder = core::QueryCtx::Builder();
    if (!inputNodeId.empty()) {
      builder.inputTransportType(inputNodeId, inputTransport);
    }
    if (!outputNodeId.empty()) {
      builder.outputTransportType(outputNodeId, outputTransport);
    }
    return builder.build();
  }

  /// Creates the plan fragment for an Exchange node.
  core::PlanFragment makeExchangePlan() {
    return PlanBuilder()
        .exchange(rowType_, VectorSerde::kindName(VectorSerde::Kind::kPresto))
        .planFragment();
  }

  /// Creates the plan fragment for a MergeExchange node.
  core::PlanFragment makeMergeExchangePlan() {
    return PlanBuilder()
        .mergeExchange(
            rowType_, {"c0"}, VectorSerde::kindName(VectorSerde::Kind::kPresto))
        .planFragment();
  }

  /// Creates the plan fragment for a PartitionedOutput node.
  core::PlanFragment makePartitionedOutputPlan() {
    auto vectors = makeRowVector(rowType_, 1);
    return PlanBuilder()
        .values({vectors})
        .partitionedOutput({"c0"}, 4)
        .planFragment();
  }

  /// Creates a Task from a plan fragment and QueryCtx.
  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      core::PlanFragment fragment,
      std::shared_ptr<core::QueryCtx> queryCtx) {
    return Task::create(
        taskId,
        std::move(fragment),
        0,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel);
  }

  /// Creates a DriverCtx pointing at the given task.
  std::shared_ptr<DriverCtx> makeDriverCtx(std::shared_ptr<Task> task) {
    return std::make_shared<DriverCtx>(
        std::move(task), 0, 0, kUngroupedGroupId, 0);
  }

  RowTypePtr rowType_{ROW({"c0", "c1"}, {BIGINT(), VARCHAR()})};

 private:
  bool savedExchange_{false};
};

TEST_F(ExchangeAdapterSelectionTest, exchangeDisabledKeepsAllOperators) {
  cudf_velox::CudfConfig::getInstance().exchange = false;

  auto plan = makeExchangePlan();
  auto planNode = plan.planNode;
  auto queryCtx = makeQueryCtx(planNode->id(), ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // With exchange disabled, the adapters' canHandle returns false,
  // so findAdapter won't match Exchange/MergeExchange/PartitionedOutput.
  auto task = makeTask("test-exchange-task", std::move(plan), queryCtx);
  auto driverCtx = makeDriverCtx(task);
  Exchange exchangeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(planNode),
      nullptr);
  EXPECT_EQ(registry.findAdapter(&exchangeOp), nullptr);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledDefaultTransportKeepsOperators) {
  auto plan = makeExchangePlan();
  auto planNode = plan.planNode;
  auto queryCtx = makeQueryCtx();
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto task = makeTask("test-exchange-task", std::move(plan), queryCtx);
  auto driverCtx = makeDriverCtx(task);
  Exchange exchangeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(planNode),
      nullptr);

  auto* adapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_TRUE(adapter->keepOperator(&exchangeOp, planNode, driverCtx.get()));

  auto props = adapter->properties(&exchangeOp, planNode, driverCtx.get());
  EXPECT_FALSE(props.canRunOnGPU);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledHttpTransportKeepsOperators) {
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // Exchange — no nodes in either map means both default to kHttp.
  auto exchangePlan = makeExchangePlan();
  auto exchangeNode = exchangePlan.planNode;
  auto exchangeQueryCtx = makeQueryCtx();
  auto exchangeTask =
      makeTask("test-exchange-task", std::move(exchangePlan), exchangeQueryCtx);
  auto exchangeDriverCtx = makeDriverCtx(exchangeTask);
  Exchange exchangeOp(
      0,
      exchangeDriverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(exchangeNode),
      nullptr);

  auto* exchangeAdapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(exchangeAdapter, nullptr);
  EXPECT_TRUE(exchangeAdapter->keepOperator(
      &exchangeOp, exchangeNode, exchangeDriverCtx.get()));

  // PartitionedOutput
  auto poPlan = makePartitionedOutputPlan();
  auto poNode = poPlan.planNode;
  auto poQueryCtx = makeQueryCtx();
  auto poTask =
      makeTask("test-partitioned-output-task", std::move(poPlan), poQueryCtx);
  auto poDriverCtx = makeDriverCtx(poTask);
  PartitionedOutput poOp(
      0,
      poDriverCtx.get(),
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(poNode),
      false);

  auto* poAdapter = registry.findAdapter(&poOp);
  ASSERT_NE(poAdapter, nullptr);
  EXPECT_TRUE(poAdapter->keepOperator(&poOp, poNode, poDriverCtx.get()));
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledUcxTransportReplacesExchange) {
  auto plan = makeExchangePlan();
  auto planNode = plan.planNode;
  auto queryCtx = makeQueryCtx(planNode->id(), ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto task = makeTask("test-exchange-task", std::move(plan), queryCtx);
  auto driverCtx = makeDriverCtx(task);
  Exchange exchangeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(planNode),
      nullptr);

  auto* adapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_FALSE(adapter->keepOperator(&exchangeOp, planNode, driverCtx.get()));

  auto props = adapter->properties(&exchangeOp, planNode, driverCtx.get());
  EXPECT_TRUE(props.canRunOnGPU);
  EXPECT_TRUE(props.producesGpuOutput);
  EXPECT_FALSE(props.acceptsGpuInput);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledUcxTransportReplacesMergeExchange) {
  auto plan = makeMergeExchangePlan();
  auto planNode = plan.planNode;
  auto queryCtx = makeQueryCtx(planNode->id(), ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto task = makeTask("test-merge-exchange-task", std::move(plan), queryCtx);
  auto driverCtx = makeDriverCtx(task);
  MergeExchange mergeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::MergeExchangeNode>(planNode));

  auto* adapter = registry.findAdapter(&mergeOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_FALSE(adapter->keepOperator(&mergeOp, planNode, driverCtx.get()));

  auto props = adapter->properties(&mergeOp, planNode, driverCtx.get());
  EXPECT_TRUE(props.canRunOnGPU);
  EXPECT_TRUE(props.producesGpuOutput);
  EXPECT_FALSE(props.acceptsGpuInput);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledUcxTransportReplacesPartitionedOutput) {
  auto plan = makePartitionedOutputPlan();
  auto planNode = plan.planNode;
  auto queryCtx = makeQueryCtx(
      "",
      ExchangeTransportType::kHttp,
      planNode->id(),
      ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto task =
      makeTask("test-partitioned-output-task", std::move(plan), queryCtx);
  auto poDriverCtx = makeDriverCtx(task);
  PartitionedOutput poOp(
      0,
      poDriverCtx.get(),
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(planNode),
      false);

  auto* adapter = registry.findAdapter(&poOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_FALSE(adapter->keepOperator(&poOp, planNode, poDriverCtx.get()));

  auto props = adapter->properties(&poOp, planNode, poDriverCtx.get());
  EXPECT_TRUE(props.canRunOnGPU);
  EXPECT_TRUE(props.acceptsGpuInput);
  EXPECT_FALSE(props.producesGpuOutput);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    partitionedOutputChecksOutputTransportNotInputTransport) {
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // Build exchange plan to get its node ID.
  auto exchangePlan = makeExchangePlan();
  auto exchangeNode = exchangePlan.planNode;

  // UCX for this exchange node's input; PO output not set → defaults to kHttp.
  auto queryCtx = makeQueryCtx(exchangeNode->id(), ExchangeTransportType::kUcx);

  // PartitionedOutput should stay CPU (output transport is HTTP).
  auto poPlan = makePartitionedOutputPlan();
  auto poNode = poPlan.planNode;
  auto poTask =
      makeTask("test-partitioned-output-task", std::move(poPlan), queryCtx);
  auto poDriverCtx = makeDriverCtx(poTask);
  PartitionedOutput poOp(
      0,
      poDriverCtx.get(),
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(poNode),
      false);

  auto* poAdapter = registry.findAdapter(&poOp);
  ASSERT_NE(poAdapter, nullptr);
  EXPECT_TRUE(poAdapter->keepOperator(&poOp, poNode, poDriverCtx.get()));

  // Exchange should be replaced (input transport is UCX for this node).
  auto exchangeTask =
      makeTask("test-exchange-task", std::move(exchangePlan), queryCtx);
  auto exchangeDriverCtx = makeDriverCtx(exchangeTask);
  Exchange exchangeOp(
      0,
      exchangeDriverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(exchangeNode),
      nullptr);

  auto* exchangeAdapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(exchangeAdapter, nullptr);
  EXPECT_FALSE(exchangeAdapter->keepOperator(
      &exchangeOp, exchangeNode, exchangeDriverCtx.get()));
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeChecksInputTransportNotOutputTransport) {
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // Build PO plan first to get its node ID for the output transport map.
  auto poPlan = makePartitionedOutputPlan();
  auto poNode = poPlan.planNode;

  // No exchange node in input map → defaults to HTTP. PO node gets UCX output.
  auto queryCtx = makeQueryCtx(
      "",
      ExchangeTransportType::kHttp,
      poNode->id(),
      ExchangeTransportType::kUcx);

  // Exchange should stay CPU (input transport defaults to HTTP).
  auto exchangePlan = makeExchangePlan();
  auto exchangeNode = exchangePlan.planNode;
  auto exchangeTask =
      makeTask("test-exchange-task", std::move(exchangePlan), queryCtx);
  auto exchangeDriverCtx = makeDriverCtx(exchangeTask);
  Exchange exchangeOp(
      0,
      exchangeDriverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(exchangeNode),
      nullptr);

  auto* exchangeAdapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(exchangeAdapter, nullptr);
  EXPECT_TRUE(exchangeAdapter->keepOperator(
      &exchangeOp, exchangeNode, exchangeDriverCtx.get()));

  // PartitionedOutput should be replaced (output transport is UCX for this
  // node).
  auto poTask =
      makeTask("test-partitioned-output-task", std::move(poPlan), queryCtx);
  auto poDriverCtx = makeDriverCtx(poTask);
  PartitionedOutput poOp(
      0,
      poDriverCtx.get(),
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(poNode),
      false);

  auto* poAdapter = registry.findAdapter(&poOp);
  ASSERT_NE(poAdapter, nullptr);
  EXPECT_FALSE(poAdapter->keepOperator(&poOp, poNode, poDriverCtx.get()));
}

} // namespace
} // namespace facebook::velox::exec::test
