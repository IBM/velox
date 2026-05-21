/*
 * Copyright (c) International Business Machines Corporation
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

#include "velox/exec/OptimizedHashPartitionFunction.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::exec;

class OptimizedHashPartitionFunctionTest : public velox::test::VectorTestBase,
                                           public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(
    OptimizedHashPartitionFunctionTest,
    powerOfTwoRangeReductionMatchesMultiplyHigh) {
  const std::vector<uint64_t> hashes = {
      0,
      1,
      0x0000'0001'0000'0000ULL,
      0x1234'5678'9abc'def0ULL,
      0xffff'ffff'ffff'ffffULL,
  };

  for (const auto numPartitions : {1, 2, 4, 1'024}) {
    std::vector<uint32_t> partitions(hashes.size());
    rangeReduction(
        hashes.data(),
        partitions.data(),
        static_cast<vector_size_t>(hashes.size()),
        numPartitions);

    std::vector<uint32_t> expected;
    expected.reserve(hashes.size());
    for (const auto hash : hashes) {
      const auto mixedHash =
          static_cast<uint32_t>(hash) ^ static_cast<uint32_t>(hash >> 32);
      expected.push_back(
          (static_cast<uint64_t>(mixedHash) * numPartitions) >> 32);
    }

    EXPECT_EQ(partitions, expected);
  }
}

TEST_F(
    OptimizedHashPartitionFunctionTest,
    optimizedHashBitRangeMatchesRegular) {
  const auto numRows = 10'000;
  auto input = makeRowVector(
      {makeNullableFlatVector<int64_t>([&] {
         std::vector<std::optional<int64_t>> values;
         values.reserve(numRows);
         for (auto row = 0; row < numRows; ++row) {
           values.emplace_back(
               row % 17 == 0 ? std::nullopt : std::optional<int64_t>(row * 13));
         }
         return values;
       }()),
       makeFlatVector<StringView>(numRows, [](auto row) {
         return StringView::makeInline(fmt::format("value_{}", row % 97));
       })});
  const auto rowType = asRowType(input->type());

  HashPartitionFunction regular(HashBitRange{0, 5}, rowType, {0, 1});
  OptimizedHashPartitionFunction optimized(HashBitRange{0, 5}, rowType, {0, 1});

  std::vector<uint32_t> regularPartitions;
  std::vector<uint32_t> optimizedPartitions;
  EXPECT_EQ(
      regular.partition(*input, regularPartitions),
      optimized.partition(*input, optimizedPartitions));
  EXPECT_EQ(regularPartitions, optimizedPartitions);
}

TEST_F(OptimizedHashPartitionFunctionTest, onePartitionReturnsConstantResult) {
  auto input = makeRowVector({makeConstant(true, 10'000)});
  const auto rowType = asRowType(input->type());
  OptimizedHashPartitionFunction partitionFunction(
      /*localExchange=*/true, 1, rowType, {0});

  std::vector<uint32_t> partitions{123};
  EXPECT_EQ(partitionFunction.partition(*input, partitions), 0u);
  EXPECT_EQ(partitions, std::vector<uint32_t>{123});
}

TEST_F(OptimizedHashPartitionFunctionTest, constantKeyReturnsConstantResult) {
  const auto numRows = 10'000;
  for (const auto& vector : {
           makeConstant(true, numRows),
           BaseVector::createNullConstant(BOOLEAN(), numRows, pool()),
       }) {
    auto input = makeRowVector({vector});
    const auto rowType = asRowType(input->type());
    OptimizedHashPartitionFunction optimized(
        /*localExchange=*/true, 16, rowType, {0});

    std::vector<uint32_t> optimizedPartitions{123};
    const auto optimizedPartition =
        optimized.partition(*input, optimizedPartitions);
    ASSERT_TRUE(optimizedPartition.has_value());
    EXPECT_LT(optimizedPartition.value(), 16);
    EXPECT_EQ(optimizedPartitions, std::vector<uint32_t>{123});
  }
}

TEST_F(OptimizedHashPartitionFunctionTest, emptyConstantKeyReturnsEmptyResult) {
  auto input = makeRowVector({makeConstant(true, 0)});
  const auto rowType = asRowType(input->type());
  OptimizedHashPartitionFunction optimized(
      /*localExchange=*/true, 16, rowType, {0});

  std::vector<uint32_t> optimizedPartitions{123};
  EXPECT_EQ(optimized.partition(*input, optimizedPartitions), std::nullopt);
  EXPECT_TRUE(optimizedPartitions.empty());
}

TEST_F(OptimizedHashPartitionFunctionTest, constantKeyMatchesFlatKey) {
  constexpr auto numRows = 10'000;
  auto constantInput = makeRowVector({makeConstant<int64_t>(123, numRows)});
  auto flatInput = makeRowVector(
      {makeFlatVector<int64_t>(numRows, [](auto /*row*/) { return 123; })});
  const auto rowType = asRowType(constantInput->type());

  for (const bool localExchange : {false, true}) {
    OptimizedHashPartitionFunction constantPartitionFunction(
        localExchange, 16, rowType, {0});
    OptimizedHashPartitionFunction flatPartitionFunction(
        localExchange, 16, rowType, {0});

    std::vector<uint32_t> constantPartitions{123};
    const auto constantPartition =
        constantPartitionFunction.partition(*constantInput, constantPartitions);
    ASSERT_TRUE(constantPartition.has_value());
    EXPECT_EQ(constantPartitions, std::vector<uint32_t>{123});

    std::vector<uint32_t> flatPartitions;
    EXPECT_EQ(
        flatPartitionFunction.partition(*flatInput, flatPartitions),
        std::nullopt);
    EXPECT_EQ(
        flatPartitions, std::vector<uint32_t>(numRows, *constantPartition));
  }
}

TEST_F(OptimizedHashPartitionFunctionTest, specUsesConfiguredImplementation) {
  auto input = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3, 4}),
       makeFlatVector<StringView>({"a", "b", "c", "d"})});
  const auto rowType = asRowType(input->type());
  HashPartitionFunctionSpec spec(rowType, std::vector<column_index_t>{0, 1});
  auto optimizedFunction = spec.create(8, /*localExchange=*/false, true);
  ASSERT_NE(
      dynamic_cast<OptimizedHashPartitionFunction*>(optimizedFunction.get()),
      nullptr);

  auto regularFunction = spec.create(8, /*localExchange=*/false);
  ASSERT_NE(
      dynamic_cast<HashPartitionFunction*>(regularFunction.get()), nullptr);

  std::vector<uint32_t> optimizedPartitions;
  ASSERT_EQ(
      optimizedFunction->partition(*input, optimizedPartitions), std::nullopt);
  ASSERT_EQ(optimizedPartitions.size(), input->size());
  for (const auto partition : optimizedPartitions) {
    EXPECT_LT(partition, 8);
  }
}
