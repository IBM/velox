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
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class CudfTopNRowNumberTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    cudf_velox::registerCudf();
  }

  void TearDown() override {
    cudf_velox::unregisterCudf();
    OperatorTestBase::TearDown();
  }

 protected:
  // Helper to run TopNRowNumber with limit=1 and compare to DuckDB
  void testTopNRowNumber(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortingKeys,
      bool generateRowNumber) {
    auto plan =
        PlanBuilder()
            .values(input)
            .topNRowNumber(partitionKeys, sortingKeys, 1, generateRowNumber)
            .planNode();

    std::string partitionClause;
    if (!partitionKeys.empty()) {
      partitionClause = "PARTITION BY " + folly::join(", ", partitionKeys);
    }

    std::string orderClause = "ORDER BY " + folly::join(", ", sortingKeys);

    // Use window function in DuckDB to get expected results
    std::string sql = fmt::format(
        "SELECT {} FROM ("
        "  SELECT *, ROW_NUMBER() OVER ({} {}) as rn FROM tmp"
        ") WHERE rn = 1",
        generateRowNumber ? "*" : "* EXCLUDE (rn)",
        partitionClause,
        orderClause);

    assertQuery(plan, sql);
  }
};

// Basic deduplication - matches user's exact pattern
// PARTITION BY c0 ORDER BY c1 DESC NULLS FIRST WHERE rn = 1
TEST_F(CudfTopNRowNumberTest, dedupDescNullsFirst) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 2, 1, 2}),
      makeNullableFlatVector<int64_t>(
          {std::nullopt, 77, 55, 44, 33, std::nullopt}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  createDuckDbTable({data});

  // Expected: partition 1 -> row with NULL (nulls first), partition 2 -> row
  // with NULL
  testTopNRowNumber({data}, {"c0"}, {"c1 DESC NULLS FIRST"}, false);
}

// Multiple partition keys (like user's 6-column key)
TEST_F(CudfTopNRowNumberTest, multiplePartitionKeys) {
  auto data = makeRowVector({
      makeFlatVector<StringView>({"A", "A", "B", "A"}),
      makeFlatVector<int32_t>({1, 1, 1, 1}),
      makeFlatVector<int64_t>({100, 200, 100, 300}), // sort key DESC
      makeFlatVector<int64_t>({10, 20, 30, 40}), // data
  });
  createDuckDbTable({data});

  // PARTITION BY c0, c1 ORDER BY c2 DESC
  testTopNRowNumber({data}, {"c0", "c1"}, {"c2 DESC"}, false);
}

// No partition keys (global dedup - take single best row)
TEST_F(CudfTopNRowNumberTest, noPartitionKeys) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({77, 33, 99, 11}), // sort key
      makeFlatVector<int64_t>({1, 2, 3, 4}), // data
  });
  createDuckDbTable({data});

  // ORDER BY c0 DESC -> returns row with c0=99
  testTopNRowNumber({data}, {}, {"c0 DESC"}, false);
}

// With row number output column
TEST_F(CudfTopNRowNumberTest, withRowNumberOutput) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 2}),
      makeFlatVector<int64_t>({100, 200, 100, 200}),
  });
  createDuckDbTable({data});

  // Verify row_number column is added with all 1s
  testTopNRowNumber({data}, {"c0"}, {"c1 DESC"}, true);
}

// Without row number output column
TEST_F(CudfTopNRowNumberTest, withoutRowNumberOutput) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2, 2}),
      makeFlatVector<int64_t>({100, 200, 100, 200}),
  });
  createDuckDbTable({data});

  // Verify no extra column added
  testTopNRowNumber({data}, {"c0"}, {"c1 DESC"}, false);
}

// Empty input
TEST_F(CudfTopNRowNumberTest, emptyInput) {
  auto data =
      makeRowVector({makeFlatVector<int64_t>({}), makeFlatVector<int64_t>({})});
  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .topNRowNumber({"c0"}, {"c1 DESC"}, 1, false)
                  .planNode();

  assertQuery(plan, "SELECT * FROM tmp WHERE 1 = 0");
}

// All rows have same partition key
TEST_F(CudfTopNRowNumberTest, singlePartition) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 1, 1, 1}),
      makeFlatVector<int64_t>({10, 50, 30, 20, 40}),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });
  createDuckDbTable({data});

  // Should return exactly 1 row (the one with max c1)
  testTopNRowNumber({data}, {"c0"}, {"c1 DESC"}, false);
}

// Each row is unique partition
TEST_F(CudfTopNRowNumberTest, allUniquePartitions) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
  });
  createDuckDbTable({data});

  // Should return all rows (no dedup needed)
  testTopNRowNumber({data}, {"c0"}, {"c1 DESC"}, false);
}

// Various sort orders
TEST_F(CudfTopNRowNumberTest, ascendingNullsLast) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 1}),
      makeNullableFlatVector<int64_t>({100, std::nullopt, 50}),
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  createDuckDbTable({data});

  testTopNRowNumber({data}, {"c0"}, {"c1 ASC NULLS LAST"}, false);
}

TEST_F(CudfTopNRowNumberTest, ascendingNullsFirst) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 1}),
      makeNullableFlatVector<int64_t>({100, std::nullopt, 50}),
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  createDuckDbTable({data});

  testTopNRowNumber({data}, {"c0"}, {"c1 ASC NULLS FIRST"}, false);
}

TEST_F(CudfTopNRowNumberTest, descendingNullsLast) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 1}),
      makeNullableFlatVector<int64_t>({100, std::nullopt, 50}),
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  createDuckDbTable({data});

  testTopNRowNumber({data}, {"c0"}, {"c1 DESC NULLS LAST"}, false);
}

// String partition key
TEST_F(CudfTopNRowNumberTest, stringPartitionKey) {
  auto data = makeRowVector({
      makeFlatVector<StringView>({"apple", "banana", "apple", "banana"}),
      makeFlatVector<int64_t>({100, 200, 300, 400}),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });
  createDuckDbTable({data});

  testTopNRowNumber({data}, {"c0"}, {"c1 DESC"}, false);
}

// Multiple sorting keys
TEST_F(CudfTopNRowNumberTest, multipleSortingKeys) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 1, 1}),
      makeFlatVector<int64_t>({100, 100, 200, 200}), // sort key 1
      makeFlatVector<int64_t>({10, 20, 30, 40}), // sort key 2 (tie breaker)
      makeFlatVector<int64_t>({1, 2, 3, 4}), // data
  });
  createDuckDbTable({data});

  testTopNRowNumber({data}, {"c0"}, {"c1 DESC", "c2 DESC"}, false);
}

// Multiple input batches
TEST_F(CudfTopNRowNumberTest, multipleBatches) {
  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < 3; ++i) {
    batches.push_back(makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3}),
        makeFlatVector<int64_t>(
            {100 + i * 10, 200 + i * 10, 300 + i * 10}), // Different values
        makeFlatVector<int64_t>({i * 3 + 1, i * 3 + 2, i * 3 + 3}),
    }));
  }
  createDuckDbTable(batches);

  testTopNRowNumber(batches, {"c0"}, {"c1 DESC"}, false);
}

// Large data test
TEST_F(CudfTopNRowNumberTest, largeData) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < 5; ++i) {
    auto partitionCol = makeFlatVector<int64_t>(
        batchSize, [](vector_size_t row) { return row % 100; });
    auto sortCol = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return batchSize * i + row; });
    auto dataCol = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return batchSize * i + row; });
    batches.push_back(makeRowVector({partitionCol, sortCol, dataCol}));
  }
  createDuckDbTable(batches);

  testTopNRowNumber(batches, {"c0"}, {"c1 DESC"}, false);
}

// Null partition keys
TEST_F(CudfTopNRowNumberTest, nullPartitionKeys) {
  auto data = makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt, 1, std::nullopt}),
      makeFlatVector<int64_t>({100, 200, 300, 400}),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });
  createDuckDbTable({data});

  testTopNRowNumber({data}, {"c0"}, {"c1 DESC"}, false);
}
