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

  /// Creates a QueryCtx with the given transport types.
  std::shared_ptr<core::QueryCtx> makeQueryCtx(
      ExchangeTransportType inputTransport = ExchangeTransportType::kHttp,
      ExchangeTransportType outputTransport = ExchangeTransportType::kHttp) {
    return core::QueryCtx::Builder()
        .inputTransportType(inputTransport)
        .outputTransportType(outputTransport)
        .build();
  }

  /// Creates a Task with an Exchange plan node and the given QueryCtx.
  std::shared_ptr<Task> makeExchangeTask(
      std::shared_ptr<core::QueryCtx> queryCtx) {
    auto planFragment =
        PlanBuilder()
            .exchange(
                rowType_, VectorSerde::kindName(VectorSerde::Kind::kPresto))
            .planFragment();
    return Task::create(
        "test-exchange-task",
        std::move(planFragment),
        0,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel);
  }

  /// Creates a Task with a MergeExchange plan node and the given QueryCtx.
  std::shared_ptr<Task> makeMergeExchangeTask(
      std::shared_ptr<core::QueryCtx> queryCtx) {
    auto planFragment =
        PlanBuilder()
            .mergeExchange(
                rowType_,
                {"c0"},
                VectorSerde::kindName(VectorSerde::Kind::kPresto))
            .planFragment();
    return Task::create(
        "test-merge-exchange-task",
        std::move(planFragment),
        0,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel);
  }

  /// Creates a Task with a PartitionedOutput plan node and the given QueryCtx.
  std::shared_ptr<Task> makePartitionedOutputTask(
      std::shared_ptr<core::QueryCtx> queryCtx) {
    auto vectors = makeRowVector(rowType_, 1);
    auto planFragment = PlanBuilder()
                            .values({vectors})
                            .partitionedOutput({"c0"}, 4)
                            .planFragment();
    return Task::create(
        "test-partitioned-output-task",
        std::move(planFragment),
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

  auto queryCtx =
      makeQueryCtx(ExchangeTransportType::kUcx, ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // With exchange disabled, the adapters' canHandle returns false,
  // so findAdapter won't match Exchange/MergeExchange/PartitionedOutput.
  auto exchangeTask = makeExchangeTask(queryCtx);
  auto driverCtx = makeDriverCtx(exchangeTask);
  auto exchangeNode = exchangeTask->planFragment().planNode;
  Exchange exchangeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(exchangeNode),
      nullptr);
  EXPECT_EQ(registry.findAdapter(&exchangeOp), nullptr);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledDefaultTransportKeepsOperators) {
  auto queryCtx = makeQueryCtx();
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto exchangeTask = makeExchangeTask(queryCtx);
  auto driverCtx = makeDriverCtx(exchangeTask);
  auto exchangeNode = exchangeTask->planFragment().planNode;
  Exchange exchangeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(exchangeNode),
      nullptr);

  auto* adapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_TRUE(
      adapter->keepOperator(&exchangeOp, exchangeNode, driverCtx.get()));

  auto props = adapter->properties(&exchangeOp, exchangeNode, driverCtx.get());
  EXPECT_FALSE(props.canRunOnGPU);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledHttpTransportKeepsOperators) {
  auto queryCtx =
      makeQueryCtx(ExchangeTransportType::kHttp, ExchangeTransportType::kHttp);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // Exchange
  auto exchangeTask = makeExchangeTask(queryCtx);
  auto exchangeDriverCtx = makeDriverCtx(exchangeTask);
  auto exchangeNode = exchangeTask->planFragment().planNode;
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
  auto poTask = makePartitionedOutputTask(queryCtx);
  auto poDriverCtx = makeDriverCtx(poTask);
  auto poNode = poTask->planFragment().planNode;
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
  auto queryCtx =
      makeQueryCtx(ExchangeTransportType::kUcx, ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto exchangeTask = makeExchangeTask(queryCtx);
  auto driverCtx = makeDriverCtx(exchangeTask);
  auto exchangeNode = exchangeTask->planFragment().planNode;
  Exchange exchangeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(exchangeNode),
      nullptr);

  auto* adapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_FALSE(
      adapter->keepOperator(&exchangeOp, exchangeNode, driverCtx.get()));

  auto props = adapter->properties(&exchangeOp, exchangeNode, driverCtx.get());
  EXPECT_TRUE(props.canRunOnGPU);
  EXPECT_TRUE(props.producesGpuOutput);
  EXPECT_FALSE(props.acceptsGpuInput);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledUcxTransportReplacesMergeExchange) {
  auto queryCtx =
      makeQueryCtx(ExchangeTransportType::kUcx, ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto mergeTask = makeMergeExchangeTask(queryCtx);
  auto driverCtx = makeDriverCtx(mergeTask);
  auto mergeNode = mergeTask->planFragment().planNode;
  MergeExchange mergeOp(
      0,
      driverCtx.get(),
      std::dynamic_pointer_cast<const core::MergeExchangeNode>(mergeNode));

  auto* adapter = registry.findAdapter(&mergeOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_FALSE(adapter->keepOperator(&mergeOp, mergeNode, driverCtx.get()));

  auto props = adapter->properties(&mergeOp, mergeNode, driverCtx.get());
  EXPECT_TRUE(props.canRunOnGPU);
  EXPECT_TRUE(props.producesGpuOutput);
  EXPECT_FALSE(props.acceptsGpuInput);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    exchangeEnabledUcxTransportReplacesPartitionedOutput) {
  auto queryCtx =
      makeQueryCtx(ExchangeTransportType::kUcx, ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  auto poTask = makePartitionedOutputTask(queryCtx);
  auto poDriverCtx = makeDriverCtx(poTask);
  auto poNode = poTask->planFragment().planNode;
  PartitionedOutput poOp(
      0,
      poDriverCtx.get(),
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(poNode),
      false);

  auto* adapter = registry.findAdapter(&poOp);
  ASSERT_NE(adapter, nullptr);
  EXPECT_FALSE(adapter->keepOperator(&poOp, poNode, poDriverCtx.get()));

  auto props = adapter->properties(&poOp, poNode, poDriverCtx.get());
  EXPECT_TRUE(props.canRunOnGPU);
  EXPECT_TRUE(props.acceptsGpuInput);
  EXPECT_FALSE(props.producesGpuOutput);
}

TEST_F(
    ExchangeAdapterSelectionTest,
    partitionedOutputChecksOutputTransportNotInputTransport) {
  // UCX for input direction, HTTP for output direction.
  auto queryCtx =
      makeQueryCtx(ExchangeTransportType::kUcx, ExchangeTransportType::kHttp);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // PartitionedOutput should stay CPU (output transport is HTTP).
  auto poTask = makePartitionedOutputTask(queryCtx);
  auto poDriverCtx = makeDriverCtx(poTask);
  auto poNode = poTask->planFragment().planNode;
  PartitionedOutput poOp(
      0,
      poDriverCtx.get(),
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(poNode),
      false);

  auto* poAdapter = registry.findAdapter(&poOp);
  ASSERT_NE(poAdapter, nullptr);
  EXPECT_TRUE(poAdapter->keepOperator(&poOp, poNode, poDriverCtx.get()));

  // Exchange should be replaced (input transport is UCX).
  auto exchangeTask = makeExchangeTask(queryCtx);
  auto exchangeDriverCtx = makeDriverCtx(exchangeTask);
  auto exchangeNode = exchangeTask->planFragment().planNode;
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
  // HTTP for input direction, UCX for output direction.
  auto queryCtx =
      makeQueryCtx(ExchangeTransportType::kHttp, ExchangeTransportType::kUcx);
  auto& registry = cudf_velox::OperatorAdapterRegistry::getInstance();

  // Exchange should stay CPU (input transport is HTTP).
  auto exchangeTask = makeExchangeTask(queryCtx);
  auto exchangeDriverCtx = makeDriverCtx(exchangeTask);
  auto exchangeNode = exchangeTask->planFragment().planNode;
  Exchange exchangeOp(
      0,
      exchangeDriverCtx.get(),
      std::dynamic_pointer_cast<const core::ExchangeNode>(exchangeNode),
      nullptr);

  auto* exchangeAdapter = registry.findAdapter(&exchangeOp);
  ASSERT_NE(exchangeAdapter, nullptr);
  EXPECT_TRUE(exchangeAdapter->keepOperator(
      &exchangeOp, exchangeNode, exchangeDriverCtx.get()));

  // PartitionedOutput should be replaced (output transport is UCX).
  auto poTask = makePartitionedOutputTask(queryCtx);
  auto poDriverCtx = makeDriverCtx(poTask);
  auto poNode = poTask->planFragment().planNode;
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
