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

#include <folly/init/Init.h>
#include <algorithm>
#include <memory>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/tests/utils/DataFiles.h" // @manual
#include "velox/dwio/parquet/RegisterParquetReader.h" // @manual
#include "velox/dwio/parquet/reader/PageReader.h" // @manual
#include "velox/dwio/parquet/reader/ParquetReader.h" // @manual=//velox/connectors/hive:velox_hive_connector_parquet
#include "velox/dwio/parquet/tests/reader/ParquetTableScanTestBase.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h" // @manual
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"
#include "velox/type/tz/TimeZoneMap.h"

#include "velox/connectors/hive/HiveConfig.h" // @manual=//velox/connectors/hive:velox_hive_connector_parquet
#include "velox/dwio/parquet/writer/Writer.h" // @manual

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::parquet;
using namespace facebook::velox::parquet::test;
using namespace facebook::velox::test;
using namespace facebook::velox::common::testutil;

namespace {

void assertDynamicFilterProduced(
    const std::shared_ptr<Task>& task,
    const core::PlanNodeId& probeScanId,
    const core::PlanNodeId& joinId) {
  const auto planStats = toPlanStats(task->taskStats());
  ASSERT_EQ(
      planStats.at(joinId).customStats.at("dynamicFiltersProduced").sum, 1);
  ASSERT_EQ(
      planStats.at(probeScanId)
          .dynamicFilterStats.producerNodeIds.count(joinId),
      1);
}

// Returns the sum of the custom stat 'name' of plan node 'nodeId' in 'task',
// or 0 if the node did not report it.
int64_t customStatSum(
    const exec::Task& task,
    const core::PlanNodeId& nodeId,
    const std::string& name) {
  const auto planStats = toPlanStats(task.taskStats());
  const auto& customStats = planStats.at(nodeId).customStats;
  const auto it = customStats.find(name);
  return it == customStats.end() ? 0 : it->second.sum;
}
} // namespace

class ParquetTableScanTest : public ParquetTableScanTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  void assertSelect(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> splits,
      std::vector<std::string>&& outputColumnNames,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder().tableScan(rowType).planNode();

    assertQuery(plan, splits, sql);
  }

  void assertSelectWithDataColumns(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> splits,
      std::vector<std::string>&& outputColumnNames,
      const RowTypePtr& dataColumns,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto plan =
        PlanBuilder().tableScan(rowType, {}, "", dataColumns).planNode();
    assertQuery(plan, splits, sql);
  }

  void assertSelectWithAssignments(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> splits,
      std::vector<std::string>&& outputColumnNames,
      const connector::ColumnHandleMap& assignments,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto plan = PlanBuilder()
                    .tableScan(rowType, {}, "", nullptr, assignments)
                    .planNode();
    assertQuery(plan, splits, sql);
  }

  void assertSelectWithFilter(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> splits,
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& subfieldFilters,
      const std::string& remainingFilter,
      const std::string& sql,
      const connector::ColumnHandleMap& assignments = {}) {
    auto rowType = getRowType(std::move(outputColumnNames));
    parse::ParseOptions options;
    options.parseDecimalAsDouble = false;

    auto plan =
        PlanBuilder(pool_.get())
            .setParseOptions(options)
            .tableScan(
                rowType, subfieldFilters, remainingFilter, nullptr, assignments)
            .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .connectorSessionProperty(
            kHiveConnectorId,
            HiveConfig::kReadTimestampUnitSession,
            std::to_string(static_cast<int>(readTimestampPrecision_)))
        .splits(splits)
        .assertResults(sql);
  }

  void assertSelectWithAgg(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> splits,
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& groupingKeys,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder()
                    .tableScan(rowType)
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();

    assertQuery(plan, splits, sql);
  }

  void assertSelectWithFilterAndAgg(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> splits,
      std::vector<std::string>&& outputColumnNames,
      const std::vector<std::string>& filters,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& groupingKeys,
      const std::string& sql) {
    auto rowType = getRowType(std::move(outputColumnNames));

    auto plan = PlanBuilder()
                    .tableScan(rowType, filters)
                    .singleAggregation(groupingKeys, aggregates)
                    .planNode();

    assertQuery(plan, splits, sql);
  }

  void assertSelectWithTimezone(
      std::vector<std::shared_ptr<connector::ConnectorSplit>> connectorSplits,
      std::vector<std::string>&& outputColumnNames,
      const std::string& sql,
      const std::string& sessionTimezone) {
    auto rowType = getRowType(std::move(outputColumnNames));
    auto plan = PlanBuilder().tableScan(rowType).planNode();
    std::vector<exec::Split> splits;
    splits.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      splits.emplace_back(folly::copy(connectorSplit), -1);
    }

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kSessionTimezone, sessionTimezone)
        .splits(splits)
        .assertResults(sql);
  }

  void loadData(RowTypePtr rowType, RowVectorPtr data) {
    rowType_ = rowType;
    createDuckDbTable({data});
  }

  void loadDataWithRowType(const std::string& filePath, RowVectorPtr data) {
    auto pool = facebook::velox::memory::memoryManager()->addLeafPool();
    dwio::common::ReaderOptions readerOpts{pool.get()};
    readerOpts.setDataIoStats(dataIoStats_);
    readerOpts.setMetadataIoStats(metadataIoStats_);
    auto reader = std::make_unique<ParquetReader>(
        std::make_unique<facebook::velox::dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(filePath), readerOpts.memoryPool()),
        readerOpts);
    rowType_ = reader->rowType();
    createDuckDbTable({data});
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return getDataFilePath(
        "velox/dwio/parquet/tests/reader", "../examples/" + fileName);
  }

  void testTimestampRead(
      const ParquetWriterOptions& options,
      TimestampPrecision readTimestampPrecision) {
    VELOX_CHECK(options.parquetWriteTimestampUnit.has_value());
    const auto [values, expectedValues] = timestampValues(
        options.parquetWriteTimestampUnit.value(), readTimestampPrecision);
    auto vector = makeRowVector(
        {"t"},
        {
            makeFlatVector<Timestamp>(values),
        });
    auto file = TempFilePath::create();
    writeToParquetFile(file->getPath(), {vector}, options);
    loadData(
        asRowType(vector->type()),
        makeRowVector(
            {"t"},
            {
                makeFlatVector<Timestamp>(expectedValues),
            }));

    readTimestampPrecision_ = readTimestampPrecision;
    auto guard = folly::makeGuard(
        [&] { readTimestampPrecision_ = kDefaultReadTimestampPrecision; });
    assertSelectWithFilter(
        {makeSplit(file->getPath())}, {"t"}, {}, "", "SELECT t from tmp");
    assertSelectWithFilter(
        {makeSplit(file->getPath())},
        {"t"},
        {},
        "t < TIMESTAMP '2000-09-12 22:36:29'",
        "SELECT t from tmp where t < TIMESTAMP '2000-09-12 22:36:29'");
    assertSelectWithFilter(
        {makeSplit(file->getPath())},
        {"t"},
        {},
        "t <= TIMESTAMP '2000-09-12 22:36:29'",
        "SELECT t from tmp where t <= TIMESTAMP '2000-09-12 22:36:29'");
    assertSelectWithFilter(
        {makeSplit(file->getPath())},
        {"t"},
        {},
        "t > TIMESTAMP '1980-01-24 00:23:07'",
        "SELECT t from tmp where t > TIMESTAMP '1980-01-24 00:23:07'");
    assertSelectWithFilter(
        {makeSplit(file->getPath())},
        {"t"},
        {},
        "t >= TIMESTAMP '1980-01-24 00:23:07'",
        "SELECT t from tmp where t >= TIMESTAMP '1980-01-24 00:23:07'");
    assertSelectWithFilter(
        {makeSplit(file->getPath())},
        {"t"},
        {},
        "t == TIMESTAMP '2022-12-23 03:56:01'",
        "SELECT t from tmp where t == TIMESTAMP '2022-12-23 03:56:01'");
    assertSelectWithFilter(
        {makeSplit(file->getPath())},
        {"t"},
        {},
        "not(eq(t, TIMESTAMP '2000-09-12 22:36:29'))",
        "SELECT t from tmp where t != TIMESTAMP '2000-09-12 22:36:29'");
  }

  void testTimestampUtcRead(
      const ParquetWriterOptions& options,
      TimestampPrecision readTimestampPrecision) {
    VELOX_CHECK(options.parquetWriteTimestampUnit.has_value());
    const auto [values, expectedValues] = timestampValues(
        options.parquetWriteTimestampUnit.value(), readTimestampPrecision);
    auto vector = makeRowVector(
        {"t"},
        {
            makeFlatVector<Timestamp>(values, TIMESTAMP_UTC()),
        });
    auto file = TempFilePath::create();
    writeToParquetFile(file->getPath(), {vector}, options);

    loadData(
        ROW({"t"}, {TIMESTAMP_UTC()}),
        makeRowVector(
            {"t"},
            {
                // Expect values are used for creating duckdb table, so keep
                // using TIMESTAMP here.
                makeFlatVector<Timestamp>(expectedValues),
            }));

    readTimestampPrecision_ = readTimestampPrecision;
    auto guard = folly::makeGuard(
        [&] { readTimestampPrecision_ = kDefaultReadTimestampPrecision; });

    assertSelectWithFilter(
        {makeSplit(file->getPath())}, {"t"}, {}, "", "SELECT t from tmp");
  }

 private:
  RowTypePtr getRowType(std::vector<std::string>&& outputColumnNames) const {
    std::vector<TypePtr> types;
    for (auto colName : outputColumnNames) {
      types.push_back(rowType_->findChild(colName));
    }

    return ROW(std::move(outputColumnNames), std::move(types));
  }

  std::pair<std::vector<Timestamp>, std::vector<Timestamp>> timestampValues(
      TimestampPrecision writeTimestampPrecision,
      TimestampPrecision readTimestampPrecision) {
    auto stringToTimestamp = [](std::string_view view) {
      return util::fromTimestampString(
                 view.data(),
                 view.size(),
                 util::TimestampParseMode::kPrestoCast)
          .value();
    };
    std::vector<std::string_view> views = {
        "2015-06-01 19:34:56.007",
        "2015-06-02 19:34:56.12306",
        "2001-02-03 03:34:06.056",
        "1998-03-01 08:01:06.996669",
        "2022-12-23 03:56:01",
        "1980-01-24 00:23:07",
        "1999-12-08 13:39:26.123456",
        "2023-04-21 09:09:34.5",
        "2000-09-12 22:36:29",
        "2007-12-12 04:27:56.999",
    };

    std::vector<Timestamp> values;
    std::vector<Timestamp> expectedValues;
    values.reserve(views.size());
    for (auto view : views) {
      auto ts = stringToTimestamp(view);
      values.emplace_back(ts);
      if (readTimestampPrecision == TimestampPrecision::kMilliseconds) {
        expectedValues.emplace_back(Timestamp::fromMillis(ts.toMillis()));
        continue;
      }
      if (readTimestampPrecision == TimestampPrecision::kMicroseconds) {
        if (writeTimestampPrecision == TimestampPrecision::kMilliseconds) {
          expectedValues.emplace_back(
              Timestamp::fromMicros(ts.toMillis() * 1'000));
          continue;
        }
        if (writeTimestampPrecision == TimestampPrecision::kMicroseconds) {
          expectedValues.emplace_back(Timestamp::fromMicros(ts.toMicros()));
          continue;
        }
      }
      VELOX_NYI(
          "Not implemented, read precision: {}, write precision: {}",
          static_cast<int>(readTimestampPrecision),
          static_cast<int>(writeTimestampPrecision));
    }
    return {values, expectedValues};
  }

  RowTypePtr rowType_;
  const TimestampPrecision kDefaultReadTimestampPrecision =
      TimestampPrecision::kMicroseconds;
  TimestampPrecision readTimestampPrecision_ = kDefaultReadTimestampPrecision;
  std::shared_ptr<io::IoStatistics> dataIoStats_ =
      std::make_shared<io::IoStatistics>();
  std::shared_ptr<io::IoStatistics> metadataIoStats_ =
      std::make_shared<io::IoStatistics>();
};

TEST_F(ParquetTableScanTest, basic) {
  loadData(
      ROW({"a", "b"}, {BIGINT(), DOUBLE()}),
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  // Plain select.
  const auto filePath = getExampleFilePath("sample.parquet");
  assertSelect({makeSplit(filePath)}, {"a"}, "SELECT a FROM tmp");
  assertSelect({makeSplit(filePath)}, {"b"}, "SELECT b FROM tmp");
  assertSelect({makeSplit(filePath)}, {"a", "b"}, "SELECT a, b FROM tmp");
  assertSelect({makeSplit(filePath)}, {"b", "a"}, "SELECT b, a FROM tmp");

  // With filters.
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a"},
      {"a < 3"},
      "",
      "SELECT a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a", "b"},
      {"a < 3"},
      "",
      "SELECT a, b FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"b", "a"},
      {"a < 3"},
      "",
      "SELECT b, a FROM tmp WHERE a < 3");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a", "b"},
      {"a < 0"},
      "",
      "SELECT a, b FROM tmp WHERE a < 0");

  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"b"},
      {"b < DOUBLE '2.0'"},
      "",
      "SELECT b FROM tmp WHERE b < 2.0");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a", "b"},
      {"b >= DOUBLE '2.0'"},
      "",
      "SELECT a, b FROM tmp WHERE b >= 2.0");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"b", "a"},
      {"b <= DOUBLE '2.0'"},
      "",
      "SELECT b, a FROM tmp WHERE b <= 2.0");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a", "b"},
      {"b < DOUBLE '0.0'"},
      "",
      "SELECT a, b FROM tmp WHERE b < 0.0");

  // With aggregations.
  assertSelectWithAgg(
      {makeSplit(filePath)}, {"a"}, {"sum(a)"}, {}, "SELECT sum(a) FROM tmp");
  assertSelectWithAgg(
      {makeSplit(filePath)}, {"b"}, {"max(b)"}, {}, "SELECT max(b) FROM tmp");
  assertSelectWithAgg(
      {makeSplit(filePath)},
      {"a", "b"},
      {"min(a)", "max(b)"},
      {},
      "SELECT min(a), max(b) FROM tmp");
  assertSelectWithAgg(
      {makeSplit(filePath)},
      {"b", "a"},
      {"max(b)"},
      {"a"},
      "SELECT max(b), a FROM tmp GROUP BY a");
  assertSelectWithAgg(
      {makeSplit(filePath)},
      {"a", "b"},
      {"max(a)"},
      {"b"},
      "SELECT max(a), b FROM tmp GROUP BY b");

  // With filter and aggregation.
  assertSelectWithFilterAndAgg(
      {makeSplit(filePath)},
      {"a"},
      {"a < 3"},
      {"sum(a)"},
      {},
      "SELECT sum(a) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {makeSplit(filePath)},
      {"a", "b"},
      {"a < 3"},
      {"sum(b)"},
      {},
      "SELECT sum(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {makeSplit(filePath)},
      {"a", "b"},
      {"a < 3"},
      {"min(a)", "max(b)"},
      {},
      "SELECT min(a), max(b) FROM tmp WHERE a < 3");
  assertSelectWithFilterAndAgg(
      {makeSplit(filePath)},
      {"b", "a"},
      {"a < 3"},
      {"max(b)"},
      {"a"},
      "SELECT max(b), a FROM tmp WHERE a < 3 GROUP BY a");
}

TEST_F(ParquetTableScanTest, lazy) {
  auto filePath = getExampleFilePath("sample.parquet");
  auto schema = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  CursorParameters params;
  params.copyResult = false;
  params.planNode = PlanBuilder().tableScan(schema).planNode();
  auto cursor = TaskCursor::create(params);
  cursor->task()->addSplit("0", exec::Split(makeSplit(filePath)));
  cursor->task()->noMoreSplits("0");
  int rows = 0;
  while (cursor->moveNext()) {
    auto* result = cursor->current()->asUnchecked<RowVector>();
    ASSERT_TRUE(result->childAt(0)->isLazy());
    ASSERT_TRUE(result->childAt(1)->isLazy());
    rows += result->size();
  }
  ASSERT_EQ(rows, 20);
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
}

TEST_F(ParquetTableScanTest, aggregatePushdown) {
  auto keysVector = makeFlatVector<int64_t>({1, 4, 0, 3, 2});
  auto valuesVector = makeFlatVector<int64_t>({8077, 6883, 5805, 10640, 3582});
  auto outputType = ROW({"c1", "c2", "c3"}, {BIGINT(), BIGINT(), BIGINT()});
  auto plan = PlanBuilder()
                  .tableScan(outputType, {"c1 = 1"}, "")
                  .singleAggregation({"c2"}, {"sum(c3)"})
                  .planNode();
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.push_back(makeSplit(getExampleFilePath("gcc_data_diff.parquet")));
  auto result = AssertQueryBuilder(plan).splits(splits).copyResults(pool());
  ASSERT_EQ(result->size(), 5);
  auto rows = result->as<RowVector>();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows->childrenSize(), 2);
  assertEqualVectors(rows->childAt(0), keysVector);
  assertEqualVectors(rows->childAt(1), valuesVector);
}

TEST_F(ParquetTableScanTest, aggregatePushdownToSmallPages) {
  const std::vector<std::string> columnNames = {"a", "b", "c"};
  const auto expectedRowVector = makeRowVector(
      {makeFlatVector<int16_t>({1, 2, 4}),
       makeFlatVector<int64_t>({7, 9, 13})});
  const auto outputType = ROW(columnNames, {SMALLINT(), SMALLINT(), VARCHAR()});
  std::vector<RowVectorPtr> data;
  for (auto row = 0; row < 10; ++row) {
    data.emplace_back(makeRowVector(
        columnNames,
        {
            makeFlatVector<int16_t>({static_cast<int16_t>(row % 5)}),
            makeFlatVector<int16_t>({static_cast<int16_t>(row)}),
            makeFlatVector<std::string>({std::to_string(row)}),
        }));
  }
  const auto filePath = TempFilePath::create();
  ParquetWriterOptions options;
  options.dataPageSize = 1;
  writeToParquetFile(filePath->getPath(), data, options);
  const auto plan =
      PlanBuilder(pool())
          .tableScan(
              outputType,
              {},
              "c <> '' AND a in (1::smallint, 2::smallint, 4::smallint)")
          .singleAggregation({"a"}, {"sum(b) as s"})
          .planNode();
  AssertQueryBuilder(plan)
      .split(makeSplit(filePath->getPath()))
      .assertResults(expectedRowVector);
}

TEST_F(ParquetTableScanTest, countStar) {
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows.
  auto filePath = getExampleFilePath("sample.parquet");
  auto split = makeSplit(filePath);

  // Output type does not have any columns.
  auto rowType = ROW({}, {});
  auto plan = PlanBuilder()
                  .tableScan(rowType)
                  .singleAggregation({}, {"count(0)"})
                  .planNode();

  assertQuery(plan, {split}, "SELECT 20");
}

TEST_F(ParquetTableScanTest, decimalSubfieldFilter) {
  // decimal.parquet holds two columns (a: DECIMAL(5, 2), b: DECIMAL(20, 5)) and
  // 20 rows (10 rows per group). Data is in plain uncompressed format:
  //   a: [100.01 .. 100.20]
  //   b: [100000000000000.00001 .. 100000000000000.00020]
  std::vector<int64_t> unscaledShortValues(20);
  std::iota(unscaledShortValues.begin(), unscaledShortValues.end(), 10001);
  loadData(
      ROW({"a"}, {DECIMAL(5, 2)}),
      makeRowVector(
          {"a"},
          {
              makeFlatVector(unscaledShortValues, DECIMAL(5, 2)),
          }));

  const auto filePath = getExampleFilePath("decimal.parquet");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a"},
      {"a < 100.07"},
      "",
      "SELECT a FROM tmp WHERE a < 100.07");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a"},
      {"a <= 100.07"},
      "",
      "SELECT a FROM tmp WHERE a <= 100.07");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a"},
      {"a > 100.07"},
      "",
      "SELECT a FROM tmp WHERE a > 100.07");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a"},
      {"a >= 100.07"},
      "",
      "SELECT a FROM tmp WHERE a >= 100.07");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a"},
      {"a = 100.07"},
      "",
      "SELECT a FROM tmp WHERE a = 100.07");
  assertSelectWithFilter(
      {makeSplit(filePath)},
      {"a"},
      {"a BETWEEN 100.07 AND 100.12"},
      "",
      "SELECT a FROM tmp WHERE a BETWEEN 100.07 AND 100.12");

  VELOX_ASSERT_THROW(
      assertSelectWithFilter(
          {makeSplit(filePath)},
          {"a"},
          {"a < 1000.7"},
          "",
          "SELECT a FROM tmp WHERE a < 1000.7"),
      "Scalar function signature is not supported: lt(DECIMAL(5, 2), DECIMAL(5, 1))");
  VELOX_ASSERT_THROW(
      assertSelectWithFilter(
          {makeSplit(filePath)},
          {"a"},
          {"a = 1000.7"},
          "",
          "SELECT a FROM tmp WHERE a = 1000.7"),
      "Scalar function signature is not supported: eq(DECIMAL(5, 2), DECIMAL(5, 1))");
}

TEST_F(ParquetTableScanTest, map) {
  auto vector = makeMapVector<StringView, StringView>({{{"name", "gluten"}}});

  loadData(
      ROW({"map"}, {MAP(VARCHAR(), VARCHAR())}),
      makeRowVector(
          {"map"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("types.parquet"))},
      {"map"},
      {},
      "",
      "SELECT map FROM tmp");
}

TEST_F(ParquetTableScanTest, nullMap) {
  loadData(
      ROW({"i", "c"}, {VARCHAR(), MAP(VARCHAR(), VARCHAR())}),
      makeRowVector(
          {"i", "c"},
          {makeConstant<std::string>("1", 1),
           makeNullableMapVector<std::string, std::string>({std::nullopt})}));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("null_map.parquet"))},
      {"i", "c"},
      {},
      "",
      "SELECT i, c FROM tmp");
}

TEST_F(ParquetTableScanTest, singleRowStruct) {
  auto vector = makeArrayVector<int32_t>({{}});
  loadData(
      ROW({"s"}, {ROW({"a", "b"}, {BIGINT(), BIGINT()})}),
      makeRowVector(
          {"s"},
          {
              vector,
          }));
  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("single_row_struct.parquet"))},
      {"s"},
      {},
      "",
      "SELECT (0, 1)");
}

TEST_F(ParquetTableScanTest, array) {
  auto vector = makeArrayVector<int32_t>({});
  loadData(
      ROW({"repeatedInt"}, {ARRAY(INTEGER())}),
      makeRowVector(
          {"repeatedInt"},
          {
              vector,
          }));

  const auto filePath = getExampleFilePath("old_repeated_int.parquet");
  assertSelectWithFilter(
      {makeSplit(filePath)}, {"repeatedInt"}, {}, "", "SELECT [1,2,3]");

  // Set the requested type for unannotated array.
  auto rowType = ROW({"repeatedInt"}, {ARRAY(INTEGER())});
  auto plan = PlanBuilder(pool_.get())
                  .tableScan(rowType, {}, "", rowType, {})
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .splits({makeSplit(filePath)})
      .assertResults("SELECT [1,2,3]");

  // Throws when reading repeated values as scalar type.
  rowType = ROW({"repeatedInt"}, {INTEGER()});
  plan = PlanBuilder(pool_.get())
             .tableScan(rowType, {}, "", rowType, {})
             .planNode();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .splits({makeSplit(filePath)})
          .assertResults(""),
      "Requested type must be array");

  rowType = ROW({"mystring"}, {ARRAY(VARCHAR())});
  plan = PlanBuilder(pool_.get())
             .tableScan(rowType, {}, "", rowType, {})
             .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .splits({makeSplit(getExampleFilePath("proto_repeated_string.parquet"))})
      .assertResults(
          "SELECT UNNEST(array[array['hello', 'world'], array['good','bye'], array['one', 'two', 'three']])");

  rowType =
      ROW({"primitive", "myComplex"},
          {INTEGER(),
           ARRAY(
               ROW({"id", "repeatedMessage"},
                   {INTEGER(), ARRAY(ROW({"someId"}, {INTEGER()}))}))});
  plan = PlanBuilder(pool_.get())
             .tableScan(rowType, {}, "", rowType, {})
             .planNode();

  // Construct the expected vector.
  auto someIdVector = makeArrayOfRowVector(
      ROW({"someId"}, {INTEGER()}),
      {
          {variant::row({3})},
          {variant::row({6})},
          {variant::row({9})},
      });
  auto rowVector = makeRowVector(
      {"id", "repeatedMessage"},
      {
          makeFlatVector<int32_t>({1, 4, 7}),
          someIdVector,
      });
  auto expected = makeRowVector(
      {"primitive", "myComplex"},
      {
          makeFlatVector<int32_t>({2, 5, 8}),
          makeArrayVector({0, 1, 2}, rowVector),
      });

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .connectorSessionProperty(
          kHiveConnectorId, FileConfig::kUseColumnNamesSession, "true")
      .splits({makeSplit(getExampleFilePath("nested_array_struct.parquet"))})
      .assertResults(expected);
}

TEST_F(ParquetTableScanTest, legacyArrayOfStructOfArray) {
  // Legacy format with Array of Struct of Array
  // ARRAY<STRUCT<ads_id: BIGINT, shop_id: BIGINT, streamer_cat_ids:
  // ARRAY<BIGINT>, items: ARRAY<BIGINT>>>.
  auto rowType =
      ROW({"ads"},
          {ARRAY(
              ROW({"ads_id", "shop_id", "streamer_cat_ids", "items"},
                  {BIGINT(), BIGINT(), ARRAY(BIGINT()), ARRAY(BIGINT())}))});

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(rowType, {}, "", rowType, {})
                  .planNode();

  auto adsArray = makeArrayVector(
      {0, 1, 2},
      makeRowVector(
          {"ads_id", "shop_id", "streamer_cat_ids", "items"},
          {makeFlatVector<int64_t>({1001, 1002}),
           makeFlatVector<int64_t>({501, 502}),
           makeArrayVector<int64_t>({{}, {11, 22}}),
           makeArrayVector<int64_t>({{9001, 9002}, {9101}})}));

  auto expected = makeRowVector({"ads"}, {adsArray});

  AssertQueryBuilder(plan)
      .split(makeSplit(
          getExampleFilePath("legacy_array_of_struct_of_array.parquet")))
      .assertResults(expected);
}

// Optional array with required elements.
TEST_F(ParquetTableScanTest, optArrayReqEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("array_0.parquet"))},
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array['c', 'd'], array['e', 'f'], array[], null])");
}

// Required array with required elements.
TEST_F(ParquetTableScanTest, reqArrayReqEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("array_1.parquet"))},
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array['c', 'd'], array[]])");
}

// Required array with optional elements.
TEST_F(ParquetTableScanTest, reqArrayOptEle) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      ROW({"_1"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("array_2.parquet"))},
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[array['a', null], array[], array[null, 'b']])");
}

TEST_F(ParquetTableScanTest, arrayOfArrayTest) {
  auto vector = makeArrayVector<StringView>({});

  loadDataWithRowType(
      getExampleFilePath("array_of_array1.parquet"),
      makeRowVector(
          {"_1"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("array_of_array1.parquet"))},
      {"_1"},
      {},
      "",
      "SELECT UNNEST(array[null, array[array['g', 'h'], null]])");
}

// Required array with legacy format.
TEST_F(ParquetTableScanTest, reqArrayLegacy) {
  auto vector = makeArrayVector<StringView>({});

  loadData(
      ROW({"element"}, {ARRAY(VARCHAR())}),
      makeRowVector(
          {"element"},
          {
              vector,
          }));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("array_3.parquet"))},
      {"element"},
      {},
      "",
      "SELECT UNNEST(array[array['a', 'b'], array[], array['c', 'd']])");
}

TEST_F(ParquetTableScanTest, filterOnNestedArray) {
  loadData(
      ROW({"struct"},
          {ROW({"a0", "a1"}, {ARRAY(VARCHAR()), ARRAY(INTEGER())})}),
      makeRowVector(
          {"unused"},
          {
              makeFlatVector<int32_t>({}),
          }));

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("struct_of_array.parquet"))},
      {"struct"},
      {},
      "struct.a0 is null",
      "SELECT ROW(NULL, NULL)");
}

// Verifies that filters derived from string function predicates, e.g.
// lower(c0) = 'aws rds' or c0 LIKE 'a_c', return the same rows as evaluating
// the predicates after the scan.
TEST_F(ParquetTableScanTest, stringFunctionFilters) {
  // Include mixed case, multi-byte characters whose byte length changes under
  // case mapping and invalid UTF-8.
  const std::vector<std::optional<std::string>> values = {
      "AWS RDS",
      "aws rds",
      "Aws Rds ",
      "AWS S3",
      "(not set)",
      "(NOT SET)",
      "",
      std::nullopt,
      "\xC3\x9C"
      "ber",
      "\xC3\xBC"
      "BER",
      "abc",
      "ABC",
      "a\xC3\xBC"
      "c",
      "\xC3\x41"
      "bc",
      // Invalid UTF-8 on which LIKE implementations disagree about how many
      // bytes one '_' matches.
      "a\x80\x80\x80\x80",
      "\xC3\xA9"
      "a",
      "\xE2\x84\xAA"
      "ab",
      "\xC8\xBA"
      "bc",
  };
  constexpr int32_t kNumRows = 10'000;
  auto data = makeRowVector(
      {"c0", "c1"},
      {
          makeNullableFlatVector<std::string>([&]() {
            std::vector<std::optional<std::string>> column;
            for (auto i = 0; i < kNumRows; ++i) {
              column.push_back(values[i % values.size()]);
            }
            return column;
          }()),
          makeFlatVector<std::string>(
              kNumRows,
              [&](auto row) {
                return row % 7 == 0 ? std::string("(Not Set)")
                                    : fmt::format("id-{}", row);
              }),
      });
  const auto rowType = asRowType(data->type());

  const std::vector<std::string> filters = {
      "lower(c0) = lower('AWS RDS')",
      "lower(c0) <> '(not set)'",
      "lower(c0) = 'aws rds' and lower(c1) <> '(not set)' and c1 is not null",
      "upper(c0) in ('ABC', '\xC3\x9C"
      "BER', '\xC8\xBA"
      "BC')",
      "lower(c0) in ('abc', 'kab')",
      "lower(c0) = 'abc' or lower(c0) = 'aws rds' or lower(c0) = 'kab'",
      "lower(c0) not in ('abc', 'aws rds')",
      "upper(c0) between 'A' and 'B'",
      "lower(c0) is null",
      "lower(upper(c0)) = 'abc'",
      "c0 like '_'",
      "c0 like '\xC3\xA9_' and c0 like '__'",
      "c0 like 'a_c'",
      "c0 like 'AWS%'",
      "c0 like '%s%'",
      "strpos(lower(c0), 'aws') > 0",
      "strpos(c1, 'Set') > 0 and lower(c0) like '%\xC3\xBC%'",
      "strpos(upper(c0), 'S R') >= 1",
      "length(c0) = 3",
      "length(c0) between 2 and 7 and lower(c0) = 'abc'",
      "length(c1) > 5",
  };

  const auto parquetFile = TempFilePath::create();
  ParquetWriterOptions parquetOptions;
  parquetOptions.enableDictionary = true;
  writeToParquetFile(parquetFile->getPath(), {data}, parquetOptions);

  const auto plainParquetFile = TempFilePath::create();
  ParquetWriterOptions plainOptions;
  plainOptions.enableDictionary = false;
  writeToParquetFile(plainParquetFile->getPath(), {data}, plainOptions);

  const auto dwrfFile = TempFilePath::create();
  writeToFile(dwrfFile->getPath(), {data});

  for (const auto& filter : filters) {
    SCOPED_TRACE(filter);

    // Evaluates the filter after the scan to get the expected rows.
    const auto expected =
        AssertQueryBuilder(
            PlanBuilder(pool()).values({data}).filter(filter).planNode())
            .copyResults(pool());
    ASSERT_GT(expected->size(), 0);

    // A split keeps the data source that reads it, so each scan needs new
    // ones.
    const std::vector<std::shared_ptr<connector::ConnectorSplit>> splits = {
        makeSplit(parquetFile->getPath()),
        makeSplit(plainParquetFile->getPath()),
        makeHiveConnectorSplit(dwrfFile->getPath()),
    };
    for (const auto& split : splits) {
      SCOPED_TRACE(split->toString());
      // The scan pushes down the filters derived from 'filter' and must
      // return the same rows.
      AssertQueryBuilder(
          PlanBuilder(pool()).tableScan(rowType, {}, filter).planNode())
          .split(split)
          .assertResults(expected);
    }
  }
}

// Verifies that filters on lower() skip row groups by their min and max.
TEST_F(ParquetTableScanTest, caseMappedFilterSkipsRowGroups) {
  constexpr int32_t kNumRows = 10'000;
  constexpr int32_t kRowsPerRowGroup = 1'000;
  constexpr int32_t kNumRowGroups = kNumRows / kRowsPerRowGroup;
  // Sorted, so each row group covers a narrow range of values.
  auto data = makeRowVector(
      {"c0"}, {makeFlatVector<std::string>(kNumRows, [](auto row) {
        return fmt::format("AB-{:05}", row);
      })});
  const auto rowType = asRowType(data->type());
  const auto file = TempFilePath::create();
  dwio::common::WriterOptions writerOptions;
  writerOptions.flushPolicyFactory = [&]() {
    return std::make_unique<DefaultFlushPolicy>(
        kRowsPerRowGroup, std::numeric_limits<int64_t>::max());
  };
  writeToParquetFile(file->getPath(), {data}, writerOptions, {});

  struct TestCase {
    std::string filter;
    int64_t numRows;
    int64_t numSkippedRowGroups;
  };
  const std::vector<TestCase> testCases = {
      {"lower(c0) = 'ab-04321'", 1, 9},
      {"lower(c0) in ('ab-00001', 'ab-09999')", 2, 8},
      {"upper(c0) = 'AB-04321'", 1, 9},
      // No string lowercases to a value with uppercase letters.
      {"lower(c0) = 'AB-04321'", 0, 10},
      {"lower(c0) = 'zz'", 0, 10},
  };
  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.filter);
    auto plan =
        PlanBuilder(pool()).tableScan(rowType, {}, testCase.filter).planNode();
    const auto scanId = plan->id();
    const auto expected = AssertQueryBuilder(PlanBuilder(pool())
                                                 .values({data})
                                                 .filter(testCase.filter)
                                                 .planNode())
                              .copyResults(pool());
    ASSERT_EQ(expected->size(), testCase.numRows);
    auto task = AssertQueryBuilder(plan)
                    .split(makeSplit(file->getPath()))
                    .assertResults(expected);
    // The split is skipped as a whole when no row group can match.
    if (testCase.numSkippedRowGroups == kNumRowGroups) {
      ASSERT_EQ(customStatSum(*task, scanId, "skippedSplits"), 1);
    } else {
      ASSERT_EQ(
          customStatSum(*task, scanId, "skippedStrides"),
          testCase.numSkippedRowGroups);
    }
  }
}

// Verifies that row groups whose dictionary has no passing value are skipped,
// including when their min and max do not exclude the value.
TEST_F(ParquetTableScanTest, dictionaryFilterSkipsRowGroups) {
  constexpr int32_t kNumRowGroups = 10;
  constexpr int32_t kRowsPerRowGroup = 1'000;
  // Row group 'g' holds 'rg<g>-AAA', 'rg<g>-CCC' and nulls, so its min and
  // max include 'rg<g>-BBB' while its dictionary does not.
  auto data = makeRowVector(
      {"c0"},
      {makeFlatVector<std::string>(
          kNumRowGroups * kRowsPerRowGroup,
          [](auto row) {
            return fmt::format(
                "rg{}-{}", row / kRowsPerRowGroup, row % 2 ? "AAA" : "CCC");
          },
          [](auto row) { return row % 7 == 0; })});
  const auto rowType = asRowType(data->type());
  dwio::common::WriterOptions writerOptions;
  writerOptions.flushPolicyFactory = [&]() {
    return std::make_unique<DefaultFlushPolicy>(
        kRowsPerRowGroup, std::numeric_limits<int64_t>::max());
  };
  const auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, writerOptions, {});
  ParquetWriterOptions plainOptions;
  plainOptions.enableDictionary = false;
  const auto plainFile = TempFilePath::create();
  writeToParquetFile(plainFile->getPath(), {data}, writerOptions, plainOptions);

  struct TestCase {
    std::string filter;
    bool plainEncoding;
    bool dictionaryFilterEnabled;
    int64_t numRows;
    int64_t numSkippedRowGroups;
  };
  // Row group 3 has 500 'rg3-AAA' rows, 72 of which are null. The file has
  // 8'571 non-null rows.
  const std::vector<TestCase> testCases = {
      {.filter = "c0 = 'rg3-BBB'",
       .plainEncoding = false,
       .dictionaryFilterEnabled = true,
       .numRows = 0,
       .numSkippedRowGroups = 10},
      {.filter = "c0 = 'rg3-BBB'",
       .plainEncoding = false,
       .dictionaryFilterEnabled = false,
       .numRows = 0,
       .numSkippedRowGroups = 9},
      {.filter = "c0 = 'rg3-BBB'",
       .plainEncoding = true,
       .dictionaryFilterEnabled = true,
       .numRows = 0,
       .numSkippedRowGroups = 9},
      {.filter = "lower(c0) = 'rg3-bbb'",
       .plainEncoding = false,
       .dictionaryFilterEnabled = true,
       .numRows = 0,
       .numSkippedRowGroups = 10},
      {.filter = "lower(c0) = 'rg3-aaa'",
       .plainEncoding = false,
       .dictionaryFilterEnabled = true,
       .numRows = 428,
       .numSkippedRowGroups = 9},
      {.filter = "c0 <> 'rg3-AAA'",
       .plainEncoding = false,
       .dictionaryFilterEnabled = true,
       .numRows = 8'143,
       .numSkippedRowGroups = 0},
      {.filter = "length(c0) = 3",
       .plainEncoding = false,
       .dictionaryFilterEnabled = true,
       .numRows = 0,
       .numSkippedRowGroups = 10},
  };
  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "{} plainEncoding {} dictionaryFilterEnabled {}",
            testCase.filter,
            testCase.plainEncoding,
            testCase.dictionaryFilterEnabled));
    auto plan =
        PlanBuilder(pool()).tableScan(rowType, {}, testCase.filter).planNode();
    const auto scanId = plan->id();
    const auto expected = AssertQueryBuilder(PlanBuilder(pool())
                                                 .values({data})
                                                 .filter(testCase.filter)
                                                 .planNode())
                              .copyResults(pool());
    ASSERT_EQ(expected->size(), testCase.numRows);
    auto task = AssertQueryBuilder(plan)
                    .connectorSessionProperty(
                        kHiveConnectorId,
                        parquetSessionProperty("dictionary_filter_enabled"),
                        testCase.dictionaryFilterEnabled ? "true" : "false")
                    .split(makeSplit(
                        testCase.plainEncoding ? plainFile->getPath()
                                               : file->getPath()))
                    .assertResults(expected);
    ASSERT_EQ(
        customStatSum(*task, scanId, "skippedStrides"),
        testCase.numSkippedRowGroups);
  }
}

// Verifies that rows in pages that the page index excludes are skipped and
// that the remaining rows of all columns are read correctly.
TEST_F(ParquetTableScanTest, pageIndexFilterSkipsRows) {
  constexpr int32_t kNumRows = 10'000;
  auto data = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<std::string>(
              kNumRows, [](auto row) { return fmt::format("AB-{:05}", row); }),
          makeFlatVector<int64_t>(kNumRows, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              kNumRows,
              [](auto row) { return fmt::format("v{}", row % 3); },
              [](auto row) { return row % 11 == 0; }),
      });
  const auto rowType = asRowType(data->type());
  // One row group of 100-row pages.
  ParquetWriterOptions options;
  options.enableWritePageIndex = true;
  // Each write batch ends a page, because it exceeds the 1-byte page size.
  options.dataPageSize = 1;
  options.batchSize = 100;
  const auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {data}, options);
  const auto noIndexFile = TempFilePath::create();
  writeToParquetFile(noIndexFile->getPath(), {data});

  struct TestCase {
    std::string filter;
    bool hasPageIndex;
    bool pageIndexFilterEnabled;
    int64_t numSkippedRows;
  };
  const std::vector<TestCase> testCases = {
      {.filter = "c0 = 'AB-04321'",
       .hasPageIndex = true,
       .pageIndexFilterEnabled = true,
       .numSkippedRows = 9'900},
      {.filter = "lower(c0) = 'ab-04321'",
       .hasPageIndex = true,
       .pageIndexFilterEnabled = true,
       .numSkippedRows = 9'900},
      {.filter = "c1 between 2050 and 2149",
       .hasPageIndex = true,
       .pageIndexFilterEnabled = true,
       .numSkippedRows = 9'800},
      {.filter = "c1 >= 2050 and c0 <= 'AB-02120'",
       .hasPageIndex = true,
       .pageIndexFilterEnabled = true,
       .numSkippedRows = 9'800},
      {.filter = "c1 in (5, 7777) and c2 = 'v0'",
       .hasPageIndex = true,
       .pageIndexFilterEnabled = true,
       .numSkippedRows = 9'800},
      // Row group statistics keep the row group but no page passes both.
      {.filter = "c1 between 150 and 160 and c0 >= 'AB-09000'",
       .hasPageIndex = true,
       .pageIndexFilterEnabled = true,
       .numSkippedRows = 10'000},
      {.filter = "c1 between 2050 and 2149",
       .hasPageIndex = true,
       .pageIndexFilterEnabled = false,
       .numSkippedRows = 0},
      {.filter = "c1 between 2050 and 2149",
       .hasPageIndex = false,
       .pageIndexFilterEnabled = true,
       .numSkippedRows = 0},
  };
  for (const auto& testCase : testCases) {
    SCOPED_TRACE(
        fmt::format(
            "{} hasPageIndex {} pageIndexFilterEnabled {}",
            testCase.filter,
            testCase.hasPageIndex,
            testCase.pageIndexFilterEnabled));
    auto plan =
        PlanBuilder(pool()).tableScan(rowType, {}, testCase.filter).planNode();
    const auto scanId = plan->id();
    const auto expected = AssertQueryBuilder(PlanBuilder(pool())
                                                 .values({data})
                                                 .filter(testCase.filter)
                                                 .planNode())
                              .copyResults(pool());
    // Small batches end reads at range boundaries and skip between batches.
    auto task = AssertQueryBuilder(plan)
                    .config(core::QueryConfig::kPreferredOutputBatchRows, "37")
                    .config(core::QueryConfig::kMaxOutputBatchRows, "37")
                    .connectorSessionProperty(
                        kHiveConnectorId,
                        parquetSessionProperty("page_index_filter_enabled"),
                        testCase.pageIndexFilterEnabled ? "true" : "false")
                    .split(makeSplit(
                        testCase.hasPageIndex ? file->getPath()
                                              : noIndexFile->getPath()))
                    .assertResults(expected);
    ASSERT_EQ(
        customStatSum(*task, scanId, "parquet.numRowsSkippedByPageIndex"),
        testCase.numSkippedRows);
  }
}

TEST_F(ParquetTableScanTest, readAsLowerCase) {
  auto vectors = {makeRowVector(
      {"A", "b"},
      {
          makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
          makeFlatVector<double>(20, [](auto row) { return row + 1; }),
      })};
  auto filePath = TempFilePath::create();
  ParquetWriterOptions options;
  writeToParquetFile(filePath->getPath(), vectors, options);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder().tableScan(ROW({"a"}, {BIGINT()})).planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .connectorSessionProperty(
          kHiveConnectorId,
          connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession,
          "true")
      .split(makeSplit(filePath->getPath()))
      .assertResults("SELECT A FROM tmp");

  // Wait for all tasks to be deleted to avoid race condition between async IO
  // preloading and TempFilePath destruction.
  waitForAllTasksToBeDeleted();

  // Test reading table with non-ascii names.
  auto vectorsNonAsciiNames = {makeRowVector(
      {"Товары", "国Ⅵ", "\uFF21", "\uFF22"},
      {
          makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
          makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          makeFlatVector<float>(20, [](auto row) { return row + 1; }),
          makeFlatVector<int32_t>(20, [](auto row) { return row + 1; }),
      })};

  auto filePath2 = TempFilePath::create();
  writeToParquetFile(filePath2->getPath(), vectorsNonAsciiNames, options);
  createDuckDbTable(vectorsNonAsciiNames);

  plan = PlanBuilder()
             .tableScan(
                 ROW({"товары", "国ⅵ", "\uFF41", "\uFF42"},
                     {BIGINT(), DOUBLE(), REAL(), INTEGER()}))
             .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .connectorSessionProperty(
          kHiveConnectorId,
          connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession,
          "true")
      .split(makeSplit(filePath2->getPath()))
      .assertResults("SELECT * FROM tmp");

  waitForAllTasksToBeDeleted();
}

TEST_F(ParquetTableScanTest, rowIndex) {
  static const char* kPath = "file_path";
  // case 1: file not have `_tmp_metadata_row_index`, scan generate it for user.
  auto filePath = getExampleFilePath("sample.parquet");
  loadData(
      ROW({"a", "b", "_tmp_metadata_row_index", kPath},
          {BIGINT(), DOUBLE(), BIGINT(), VARCHAR()}),
      makeRowVector(
          {"a", "b", "_tmp_metadata_row_index", kPath},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
              makeFlatVector<int64_t>(20, [](auto row) { return row; }),
              makeFlatVector<std::string>(
                  20, [filePath](auto row) { return filePath; }),
          }));
  connector::ColumnHandleMap assignments;
  assignments["a"] = std::make_shared<connector::hive::HiveColumnHandle>(
      "a",
      connector::hive::FileColumnHandle::ColumnType::kRegular,
      BIGINT(),
      BIGINT());
  assignments["b"] = std::make_shared<connector::hive::HiveColumnHandle>(
      "b",
      connector::hive::FileColumnHandle::ColumnType::kRegular,
      DOUBLE(),
      DOUBLE());
  assignments[kPath] = synthesizedColumn(kPath, VARCHAR());
  assignments["_tmp_metadata_row_index"] =
      std::make_shared<connector::hive::HiveColumnHandle>(
          "_tmp_metadata_row_index",
          connector::hive::FileColumnHandle::ColumnType::kRowIndex,
          BIGINT(),
          BIGINT());

  assertSelect(
      {makeSplit(
          filePath,
          std::nullopt,
          std::unordered_map<std::string, std::string>{{kPath, filePath}})},
      {"a"},
      "SELECT a FROM tmp");
  assertSelectWithAssignments(
      {makeSplit(
          filePath,
          std::nullopt,
          std::unordered_map<std::string, std::string>{{kPath, filePath}})},
      {"a", "_tmp_metadata_row_index"},
      assignments,
      "SELECT a, _tmp_metadata_row_index FROM tmp");
  assertSelectWithAssignments(
      {makeSplit(
          filePath,
          std::nullopt,
          std::unordered_map<std::string, std::string>{{kPath, filePath}})},
      {"_tmp_metadata_row_index", "a"},
      assignments,
      "SELECT _tmp_metadata_row_index, a FROM tmp");
  assertSelectWithAssignments(
      {makeSplit(
          filePath,
          std::nullopt,
          std::unordered_map<std::string, std::string>{{kPath, filePath}})},
      {"_tmp_metadata_row_index"},
      assignments,
      "SELECT _tmp_metadata_row_index FROM tmp");
  assertSelectWithAssignments(
      {makeSplit(
          filePath,
          std::nullopt,
          std::unordered_map<std::string, std::string>{{kPath, filePath}})},
      {kPath, "_tmp_metadata_row_index"},
      assignments,
      fmt::format("SELECT {}, _tmp_metadata_row_index FROM tmp", kPath));

  // case 2: file has `_tmp_metadata_row_index` column, then use user data
  // insteads of generating it.
  loadData(
      ROW({"a", "b", "_tmp_metadata_row_index"},
          {BIGINT(), DOUBLE(), BIGINT()}),
      makeRowVector(
          {"a", "b", "_tmp_metadata_row_index"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
          }));

  filePath = getExampleFilePath("sample_with_rowindex.parquet");
  assertSelect({makeSplit(filePath)}, {"a"}, "SELECT a FROM tmp");
  assertSelect(
      {makeSplit(filePath)},
      {"a", "_tmp_metadata_row_index"},
      "SELECT a, _tmp_metadata_row_index FROM tmp");
}

// The file icebergNullIcebergPartition.parquet was copied from a null
// partition in an Iceberg table created with the below DDL using Spark:
//
// CREATE TABLE iceberg_tmp_parquet_partitioned
//    ( c0 bigint, c1 bigint )
// USING iceberg
// PARTITIONED BY (c1)
// TBLPROPERTIES ('write.format.default' = 'parquet', 'format-version' = 2,
// 'write.delete.mode' = 'merge-on-read') LOCATION
// 's3a://presto-workload/tmp/iceberg_tmp_parquet_partitioned';
//
// INSERT INTO iceberg_tmp_parquet_partitioned
// VALUES (1, 1), (2, null),(3, null);
TEST_F(ParquetTableScanTest, filterNullIcebergPartition) {
  loadData(
      ROW({"c0", "c1"}, {BIGINT(), BIGINT()}),
      makeRowVector(
          {"c0", "c1"},
          {
              makeFlatVector<int64_t>(std::vector<int64_t>{2, 3}),
              makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}),
          }));

  std::shared_ptr<connector::ColumnHandle> c0 = makeColumnHandle(
      "c0", BIGINT(), BIGINT(), {}, FileColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c1 = makeColumnHandle(
      "c1",
      BIGINT(),
      BIGINT(),
      {},
      FileColumnHandle::ColumnType::kPartitionKey);

  const auto filePath =
      getExampleFilePath("icebergNullIcebergPartition.parquet");
  assertSelectWithFilter(
      {makeSplit(
          filePath,
          std::unordered_map<std::string, std::optional<std::string>>{
              {"c1", std::nullopt}})},
      {"c0", "c1"},
      {"c1 IS NOT NULL"},
      "",
      "SELECT c0, c1 FROM tmp WHERE c1 IS NOT NULL",
      connector::ColumnHandleMap{{"c0", c0}, {"c1", c1}});

  assertSelectWithFilter(
      {makeSplit(
          filePath,
          std::unordered_map<std::string, std::optional<std::string>>{
              {"c1", std::nullopt}})},
      {"c0", "c1"},
      {"c1 IS NULL"},
      "",
      "SELECT c0, c1 FROM tmp WHERE c1 IS NULL",
      connector::ColumnHandleMap{{"c0", c0}, {"c1", c1}});
}

TEST_F(ParquetTableScanTest, sessionTimezone) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::parquet::PageReader::readPageHeader",
      std::function<void(PageReader*)>(([&](PageReader* reader) {
        VELOX_CHECK_EQ(reader->sessionTimezone()->name(), "Asia/Shanghai");
      })));

  // Read sample.parquet to verify if the sessionTimezone in the PageReader
  // meets expectations.
  loadData(
      ROW({"a", "b"}, {BIGINT(), DOUBLE()}),
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  assertSelectWithTimezone(
      {makeSplit(getExampleFilePath("sample.parquet"))},
      {"a"},
      "SELECT a FROM tmp",
      "Asia/Shanghai");
}

TEST_F(ParquetTableScanTest, timestampInt64DictionaryMicro) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = true;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampInt64DictionaryMilli) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = true;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMilliseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampInt64PlainMicro) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = false;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampInt64PlainMilli) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = false;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMilliseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampInt96DictionaryMicro) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = true;
  options.enableDictionary = true;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampInt96DictionaryMilli) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = true;
  options.enableDictionary = true;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMilliseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampInt96PlainMicro) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = true;
  options.enableDictionary = false;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampInt96PlainMilli) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = true;
  options.enableDictionary = false;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMilliseconds;
  testTimestampRead(options, TimestampPrecision::kMicroseconds);
  testTimestampRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampConvertedType) {
  auto stringToTimestamp = [](std::string_view view) {
    return util::fromTimestampString(
               view.data(), view.size(), util::TimestampParseMode::kPrestoCast)
        .value();
  };
  std::vector<std::string_view> expected = {
      "1970-01-01 00:00:00.010",
      "1970-01-01 00:00:00.010",
      "1970-01-01 00:00:00.010",
  };
  std::vector<Timestamp> values;
  values.reserve(expected.size());
  for (auto view : expected) {
    values.emplace_back(stringToTimestamp(view));
  }

  const auto vector = makeRowVector(
      {"time"},
      {
          makeFlatVector<Timestamp>(values),
      });
  const auto schema = asRowType(vector->type());
  loadData(schema, vector);

  assertSelectWithFilter(
      {makeSplit(getExampleFilePath("tmmillis_i64.parquet"))},
      {"time"},
      {},
      "",
      "SELECT time from tmp");
}

TEST_F(ParquetTableScanTest, timestampPrecisionMicrosecond) {
  // Write timestamp data into parquet.
  constexpr int kSize = 10;
  auto vector = makeRowVector({
      makeFlatVector<Timestamp>(
          kSize, [](auto i) { return Timestamp(i, i * 1'001'001); }),
  });
  auto schema = asRowType(vector->type());
  for (const auto writeInt96 : {true, false}) {
    auto file = TempFilePath::create();
    ParquetWriterOptions options;
    options.writeInt96AsTimestamp = writeInt96;
    writeToParquetFile(file->getPath(), {vector}, options);
    auto plan = PlanBuilder().tableScan(schema).planNode();

    // Read timestamp data from parquet with microsecond precision.
    auto split = makeSplit(file->getPath());
    auto result =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .connectorSessionProperty(
                kHiveConnectorId, HiveConfig::kReadTimestampUnitSession, "6")
            .split(split)
            .copyResults(pool());
    auto expected = makeRowVector({
        makeFlatVector<Timestamp>(
            kSize, [](auto i) { return Timestamp(i, i * 1'001'000); }),
    });
    assertEqualResults({expected}, {result});
  }
}

TEST_F(ParquetTableScanTest, timestampUtcPlainMicro) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = false;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  testTimestampUtcRead(options, TimestampPrecision::kMicroseconds);
  testTimestampUtcRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampUtcDictionaryMicro) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = true;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  testTimestampUtcRead(options, TimestampPrecision::kMicroseconds);
  testTimestampUtcRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampUtcPlainMilli) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = false;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMilliseconds;
  testTimestampUtcRead(options, TimestampPrecision::kMicroseconds);
  testTimestampUtcRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, timestampUtcDictionaryMilli) {
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  options.enableDictionary = true;
  options.parquetWriteTimestampUnit = TimestampPrecision::kMilliseconds;
  testTimestampUtcRead(options, TimestampPrecision::kMicroseconds);
  testTimestampUtcRead(options, TimestampPrecision::kMilliseconds);
}

TEST_F(ParquetTableScanTest, testColumnNotExists) {
  auto rowType =
      ROW({"a", "b", "not_exists", "not_exists_array", "not_exists_map"},
          {BIGINT(),
           DOUBLE(),
           BIGINT(),
           ARRAY(VARBINARY()),
           MAP(VARCHAR(), BIGINT())});
  // message schema {
  //  optional int64 a;
  //  optional double b;
  // }
  loadData(
      rowType,
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
              makeFlatVector<double>(20, [](auto row) { return row + 1; }),
          }));

  assertSelectWithDataColumns(
      {makeSplit(getExampleFilePath("sample.parquet"))},
      {"a", "b", "not_exists", "not_exists_array", "not_exists_map"},
      rowType,
      "SELECT a, b, NULL, NULL, NULL FROM tmp");
}

// The filters reference whole structs without projecting or filtering their
// children. The first struct has a repeated branch before a scalar sibling;
// the second has scalar children only.
TEST_F(ParquetTableScanTest, structFilterByIndex) {
  const auto id = makeFlatVector<int64_t>({1, 2, 3});
  const auto detailElements = makeRowVector(
      {"first", "last"},
      {
          makeFlatVector<std::string>({"Janet", "John", "Susan"}),
          makeFlatVector<std::string>({"Jones", "Smith", "Lee"}),
      });
  const auto details =
      makeArrayVector(std::vector<vector_size_t>{0, 2, 2}, detailElements);
  const auto name = makeRowVector(
      {"details", "suffix"},
      {
          // A repeated branch appears first in schema order, but a scalar
          // sibling can provide the struct's levels with less added I/O.
          details,
          makeFlatVector<std::string>({"Jr.", "Sr.", "III"}),
      },
      [](auto row) { return row == 1; });
  const auto nickname = makeRowVector(
      {"value", "suffix"},
      {
          makeFlatVector<std::string>({"Jan", "Johnny", "Sue"}),
          makeFlatVector<std::string>({"J.", "S.", "L."}),
      },
      [](auto row) { return row == 2; });
  const auto address = makeFlatVector<std::string>(
      {"123 Main Street", "567 Maple Drive", "789 Oak Avenue"});
  const auto vector = makeRowVector(
      {"id", "name", "nickname", "address"}, {id, name, nickname, address});
  dwio::common::WriterOptions writerOptions;
  writerOptions.flushPolicyFactory = []() {
    return std::make_unique<DefaultFlushPolicy>(
        /*rowsInRowGroup=*/2, DefaultFlushPolicy::kDefaultBytesInRowGroup);
  };
  ParquetWriterOptions options;
  const auto file = TempFilePath::create();
  // Split the three rows across two row groups to verify that the hidden leaf
  // reader is enqueued and repositioned when the row group changes.
  writeToParquetFile(
      file->getPath(), {vector}, std::move(writerOptions), options);

  const auto dataColumns = vector->rowType();
  const auto outputType = ROW("id", BIGINT());
  const auto plan = PlanBuilder()
                        .tableScan(
                            outputType,
                            {},
                            "not(is_null(name)) AND not(is_null(nickname))",
                            dataColumns)
                        .planNode();
  const auto expected = makeRowVector({"id"}, {makeFlatVector<int64_t>({1})});
  AssertQueryBuilder(plan)
      // The null-struct policy is name-mapping-only. Empty child specs under
      // position mapping must preserve the physical structs' nullness.
      .connectorSessionProperty(
          kHiveConnectorId,
          parquetSessionProperty(
              ParquetConfig::kNullStructIfAllFieldsMissingSession),
          "true")
      .split(makeSplit(file->getPath()))
      .assertResults(expected);
}

// A MAP is the only physical child of a struct with no projected fields. Two
// entries in the first row must not shift the following null struct, and an
// empty map must still leave its enclosing struct non-null.
TEST_F(ParquetTableScanTest, mapStructFilterByIndex) {
  const auto id = makeFlatVector<int64_t>({1, 2, 3, 4});
  const auto name = makeRowVector(
      {"lookup"},
      {
          // The MAP supplies repetition and definition levels for the
          // enclosing struct.
          makeMapVector(
              {0, 2, 2, 2},
              makeFlatVector<int64_t>({10, 20, 30}),
              makeFlatVector<bool>({true, false, true})),
      },
      [](auto row) { return row == 1; });
  const auto vector = makeRowVector({"id", "name"}, {id, name});
  dwio::common::WriterOptions writerOptions;
  writerOptions.flushPolicyFactory = []() {
    return std::make_unique<DefaultFlushPolicy>(
        /*rowsInRowGroup=*/2, DefaultFlushPolicy::kDefaultBytesInRowGroup);
  };
  const auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, std::move(writerOptions), {});

  const auto outputType = ROW("id", BIGINT());
  const auto plan =
      PlanBuilder()
          .tableScan(outputType, {}, "not(is_null(name))", vector->rowType())
          .planNode();
  const auto expected =
      makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 3, 4})});
  AssertQueryBuilder(plan)
      .split(makeSplit(file->getPath()))
      .assertResults(expected);
}

// ARRAY elements and MAP values are structs with no projected fields. Recursive
// row-group setup must follow each struct's synthetic rep/def source through
// the repeated reader to preserve element and value nullness.
TEST_F(ParquetTableScanTest, structElementsByName) {
  const auto id = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  const auto names = makeRowVector(
      {"first", "last"},
      {
          makeFlatVector<std::string>({
              "Janet",
              "John",
              "Susan",
              "Martha",
              "Alex",
          }),
          makeFlatVector<std::string>({
              "Jones",
              "Smith",
              "Lee",
              "Taylor",
              "Wong",
          }),
      },
      [](auto row) { return row == 1 || row == 4; });
  // Rows cover multiple elements, empty collections, a null collection, and
  // null struct elements or values. The final row is in its own row group.
  const std::vector<vector_size_t> offsets = {0, 2, 2, 2, 2};
  const auto vector = makeRowVector(
      {"id", "names", "lookup"},
      {
          id,
          makeArrayVector(offsets, names, {3}),
          vectorMaker_.mapVector(
              offsets, makeFlatVector<int64_t>({1, 2, 3, 4, 5}), names, {3}),
      });

  dwio::common::WriterOptions writerOptions;
  writerOptions.flushPolicyFactory = []() {
    return std::make_unique<DefaultFlushPolicy>(
        /*rowsInRowGroup=*/2, DefaultFlushPolicy::kDefaultBytesInRowGroup);
  };
  ParquetWriterOptions options;
  const auto file = TempFilePath::create();
  writeToParquetFile(
      file->getPath(), {vector}, std::move(writerOptions), options);

  const auto emptyRowType = ROW({}, {});
  const auto outputType =
      ROW({"id", "names", "lookup"},
          {BIGINT(), ARRAY(emptyRowType), MAP(BIGINT(), emptyRowType)});
  const auto plan = PlanBuilder()
                        .startTableScan()
                        .outputType(outputType)
                        .dataColumns(outputType)
                        .endTableScan()
                        .planNode();

  const auto emptyRows = makeRowVector(emptyRowType, 5);
  setNulls(emptyRows, [](auto row) { return row == 1 || row == 4; });
  const auto expected = makeRowVector(
      {"id", "names", "lookup"},
      {
          id,
          makeArrayVector(offsets, emptyRows, {3}),
          vectorMaker_.mapVector(
              offsets,
              makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
              emptyRows,
              {3}),
      });
  AssertQueryBuilder(plan)
      // Force multiple reads within each row group so preset nulls and lengths
      // are consumed and refreshed without a row-group transition.
      .config(core::QueryConfig::kPreferredOutputBatchRows, "1")
      .config(core::QueryConfig::kMaxOutputBatchRows, "1")
      .connectorSessionProperty(
          kHiveConnectorId, FileConfig::kUseColumnNamesSession, "true")
      .split(makeSplit(file->getPath()))
      .assertResults(expected);
}

TEST_F(ParquetTableScanTest, deltaByteArray) {
  auto a = makeFlatVector<StringView>({"axis", "axle", "babble", "babyhood"});
  auto expected = makeRowVector({"a"}, {a});
  createDuckDbTable("expected", {expected});

  auto vector = makeFlatVector<StringView>({{}});
  loadData(ROW({"a"}, {VARCHAR()}), makeRowVector({"a"}, {vector}));
  assertSelect(
      {makeSplit(getExampleFilePath("delta_byte_array.parquet"))},
      {"a"},
      "SELECT a from expected");
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedConstantDelta) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding =
      facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;

  constexpr vector_size_t kSize = 1024;
  auto vector = makeRowVector(
      {"c"},
      {makeFlatVector<int64_t>(kSize, [](auto row) { return 100 + 7 * row; })});
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(vector->rowType(), vector);

  assertSelect({makeSplit(file->getPath())}, {"c"}, "SELECT c FROM tmp");
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedNarrowBitWidth) {
  auto run = [&]<typename T>() {
    constexpr vector_size_t kSize = 4096;
    auto vector = makeRowVector({"c"}, {makeFlatVector<T>(kSize, [](auto row) {
                                  return static_cast<T>(row + row / 2);
                                })});

    for (bool useV2 : {false, true}) {
      SCOPED_TRACE(fmt::format("T=int{} useV2={}", sizeof(T) * 8, useV2));
      ParquetWriterOptions options;
      options.enableDictionary = false;
      options.encoding =
          facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;
      options.useParquetDataPageV2 = useV2;
      dwio::common::WriterOptions writerOptions;
      writerOptions.compressionKind =
          common::CompressionKind::CompressionKind_NONE;

      auto file = TempFilePath::create();
      writeToParquetFile(
          file->getPath(), {vector}, std::move(writerOptions), options);
      loadData(vector->rowType(), vector);
      assertSelect({makeSplit(file->getPath())}, {"c"}, "SELECT c FROM tmp");
    }
  };

  run.template operator()<int32_t>();
  run.template operator()<int64_t>();
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedBitWidth32) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding =
      facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;

  constexpr vector_size_t kSize = 1024;
  constexpr int64_t kStep = (1LL << 32) - 1;
  auto vector = makeRowVector(
      {"c"}, {makeFlatVector<int64_t>(kSize, [](auto row) {
        return static_cast<int64_t>((row / 2) * kStep + (row % 2 ? kStep : 0));
      })});
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(vector->rowType(), vector);

  assertSelect({makeSplit(file->getPath())}, {"c"}, "SELECT c FROM tmp");
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedBitWidthSweep) {
  auto run = [&]<typename T>(int maxBitWidth) {
    SCOPED_TRACE(fmt::format("T=int{}", sizeof(T) * 8));
    constexpr vector_size_t kSize = 256;
    std::vector<std::string> names;
    std::vector<VectorPtr> children;
    for (int bw = 1; bw <= maxBitWidth; ++bw) {
      const int64_t step = (bw == 32) ? 0xFFFFFFFFLL : ((1LL << bw) - 1);
      children.push_back(makeFlatVector<T>(kSize, [step](auto row) -> T {
        return static_cast<T>((row / 2) * step + ((row % 2) ? step : 0));
      }));
      names.push_back(fmt::format("c{}", bw));
    }
    auto vector = makeRowVector(names, children);

    ParquetWriterOptions options;
    options.enableDictionary = false;
    options.encoding =
        facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;
    auto file = TempFilePath::create();
    writeToParquetFile(file->getPath(), {vector}, options);
    loadData(vector->rowType(), vector);

    assertSelect(
        {makeSplit(file->getPath())}, std::move(names), "SELECT * FROM tmp");
  };

  run.template operator()<int32_t>(31);
  run.template operator()<int64_t>(32);
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedBitWidth33) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding =
      facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;

  constexpr vector_size_t kSize = 1024;
  constexpr int64_t kStep = 1LL << 32;
  auto vector = makeRowVector(
      {"c"}, {makeFlatVector<int64_t>(kSize, [](auto row) {
        return static_cast<int64_t>((row / 2) * kStep + (row % 2 ? kStep : 0));
      })});
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(vector->rowType(), vector);

  assertSelect({makeSplit(file->getPath())}, {"c"}, "SELECT c FROM tmp");
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedMixedMiniblockWidths) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding =
      facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;

  constexpr vector_size_t kSize = 128;
  auto vector =
      makeRowVector({"c"}, {makeFlatVector<int64_t>(kSize, [](auto row) {
                      auto deltaForRow = [](vector_size_t r) -> int64_t {
                        const int mb = (r / 32) % 4;
                        const int parity = r % 2;
                        if (mb == 0)
                          return parity;
                        if (mb == 1)
                          return parity ? 200LL : 0LL;
                        if (mb == 2)
                          return parity ? 50'000LL : 0LL;
                        return parity ? 12'000'000LL : 0LL;
                      };
                      int64_t v = 0;
                      for (vector_size_t i = 0; i < row; ++i) {
                        v += deltaForRow(i);
                      }
                      return v;
                    })});
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(vector->rowType(), vector);

  assertSelect({makeSplit(file->getPath())}, {"c"}, "SELECT c FROM tmp");
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedFilterScalarTail) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding =
      facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;
  options.dataPageSize = 8 * 1024;
  options.batchSize = 1024;

  constexpr vector_size_t kSize = 128 * 1024;
  auto vector = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>(
           kSize, [](auto row) { return static_cast<int64_t>(row); }),
       makeFlatVector<int64_t>(
           kSize, [](auto row) { return 100'000LL + row * 31LL; })});
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(vector->rowType(), vector);

  assertSelectWithFilter(
      {makeSplit(file->getPath())},
      {"a", "b"},
      {"a BETWEEN 10000 AND 70000", "b > 200000"},
      "",
      "SELECT a, b FROM tmp WHERE a BETWEEN 10000 AND 70000 AND b > 200000");
}

TEST_F(ParquetTableScanTest, deltaBinaryPackedWideAndNegative) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding =
      facebook::velox::parquet::arrow::Encoding::kDeltaBinaryPacked;

  constexpr vector_size_t kSize = 1024;
  auto vector = makeRowVector(
      {"c"}, {makeFlatVector<int64_t>(kSize, [](auto row) {
        const int64_t steps[] = {
            -1'000'000'000LL, 2'000'000'000LL, -500'000LL, 1'500'000'000LL};
        return steps[row % 4] * row;
      })});
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(vector->rowType(), vector);

  assertSelect({makeSplit(file->getPath())}, {"c"}, "SELECT c FROM tmp");
}

TEST_F(ParquetTableScanTest, booleanRle) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding = facebook::velox::parquet::arrow::Encoding::kRle;
  options.useParquetDataPageV2 = true;

  auto allTrue = [](vector_size_t row) -> bool { return true; };
  auto allFalse = [](vector_size_t row) -> bool { return false; };
  auto nonNullAtFirst = [](vector_size_t row) -> bool { return row != 0; };
  auto randomTrueFalse = [](vector_size_t row) -> bool {
    return std::rand() % 2 == 0;
  };
  auto randomNull = [](vector_size_t row) -> bool {
    return std::rand() % 2 == 0;
  };

  auto vector = makeRowVector(
      {"c0", "c1", "c2", "c3", "c4"},
      {
          makeFlatVector<bool>(100, allTrue, nonNullAtFirst),
          makeFlatVector<bool>(100, allFalse, nonNullAtFirst),
          makeFlatVector<bool>(100, allTrue),
          makeFlatVector<bool>(100, allFalse),
          makeFlatVector<bool>(100, randomTrueFalse, randomNull),
      });
  auto schema = asRowType(vector->type());
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(schema, vector);

  std::shared_ptr<connector::ColumnHandle> c0 = makeColumnHandle(
      "c0", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c1 = makeColumnHandle(
      "c1", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c2 = makeColumnHandle(
      "c2", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c3 = makeColumnHandle(
      "c3", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c4 = makeColumnHandle(
      "c4", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);

  assertSelect({makeSplit(file->getPath())}, {"c0"}, "SELECT c0 FROM tmp");
  assertSelect({makeSplit(file->getPath())}, {"c1"}, "SELECT c1 FROM tmp");
  assertSelect({makeSplit(file->getPath())}, {"c2"}, "SELECT c2 FROM tmp");
  assertSelect({makeSplit(file->getPath())}, {"c3"}, "SELECT c3 FROM tmp");
  assertSelect({makeSplit(file->getPath())}, {"c4"}, "SELECT c4 FROM tmp");
}

TEST_F(ParquetTableScanTest, singleBooleanRle) {
  ParquetWriterOptions options;
  options.enableDictionary = false;
  options.encoding = facebook::velox::parquet::arrow::Encoding::kRle;
  options.useParquetDataPageV2 = true;

  auto vector = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<bool>(std::vector<bool>{true}),
          makeFlatVector<bool>(std::vector<bool>{false}),
          makeNullableFlatVector<bool>({std::nullopt}),
      });
  auto schema = asRowType(vector->type());
  auto file = TempFilePath::create();
  writeToParquetFile(file->getPath(), {vector}, options);
  loadData(schema, vector);

  std::shared_ptr<connector::ColumnHandle> c0 = makeColumnHandle(
      "c0", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c1 = makeColumnHandle(
      "c1", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);
  std::shared_ptr<connector::ColumnHandle> c2 = makeColumnHandle(
      "c2", BOOLEAN(), BOOLEAN(), {}, FileColumnHandle::ColumnType::kRegular);

  assertSelect({makeSplit(file->getPath())}, {"c0"}, "SELECT c0 FROM tmp");
  assertSelect({makeSplit(file->getPath())}, {"c1"}, "SELECT c1 FROM tmp");
  assertSelect({makeSplit(file->getPath())}, {"c2"}, "SELECT c2 FROM tmp");
}

TEST_F(ParquetTableScanTest, intToBigintRead) {
  vector_size_t kSize = 100;
  RowVectorPtr intDataFileVectors = makeRowVector(
      {"c1"}, {makeFlatVector<int32_t>(kSize, [](auto row) { return row; })});

  RowVectorPtr bigintDataFileVectors = makeRowVector(
      {"c1"}, {makeFlatVector<int64_t>(kSize, [](auto row) { return row; })});

  const std::shared_ptr<TempDirectoryPath> dataFileFolder =
      TempDirectoryPath::create();
  auto filePath = dataFileFolder->getPath() + "/" + "data.parquet";
  ParquetWriterOptions options;
  options.writeInt96AsTimestamp = false;
  writeToParquetFile(filePath, {intDataFileVectors}, options);

  auto rowType = ROW({"c1"}, {BIGINT()});
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .dataColumns(rowType)
                .endTableScan()
                .planNode();

  auto split = makeSplit(filePath);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
  auto rows = result->as<RowVector>();

  assertEqualVectors(bigintDataFileVectors->childAt(0), rows->childAt(0));
}

TEST_F(ParquetTableScanTest, intNarrowingRejectedByDefault) {
  // Narrowing conversions are rejected when allowInt32Narrowing is false
  // (default). Each case writes a wider integer column and reads as a narrower
  // type, expecting an exception from convertType.
  auto assertNarrowingThrows = [&](const VectorPtr& sourceVector,
                                   const TypePtr& targetType) {
    auto vectors = makeRowVector({"c1"}, {sourceVector});
    auto dataFile = TempFilePath::create();
    writeToParquetFile(dataFile->getPath(), {vectors}, ParquetWriterOptions{});
    auto rowType = ROW({"c1"}, {targetType});
    auto op = PlanBuilder()
                  .startTableScan()
                  .outputType(rowType)
                  .dataColumns(rowType)
                  .endTableScan()
                  .planNode();
    VELOX_ASSERT_THROW(
        AssertQueryBuilder(op)
            .split(makeSplit(dataFile->getPath()))
            .copyResults(pool()),
        "is not allowed for requested type");
  };

  assertNarrowingThrows(makeFlatVector<int16_t>({1, 2}), TINYINT());
  assertNarrowingThrows(makeFlatVector<int32_t>({1, 2}), SMALLINT());
  assertNarrowingThrows(makeFlatVector<int32_t>({1, 2}), TINYINT());
}

TEST_F(ParquetTableScanTest, intReadWithNarrowerType) {
  // Reading a wider integer as a narrower one causes unchecked truncation and
  // two's complement reinterpretation, resulting in values INT_MAX becoming -1.
  // Only INT32 physical type narrowing is supported (INT_16 -> TINYINT,
  // INT_32 -> SMALLINT/TINYINT). INT64 -> INT32 is not allowed.
  RowVectorPtr intVectors = makeRowVector(
      {"c1", "c2", "c3"},
      {
          makeFlatVector<int16_t>(
              {123,
               std::numeric_limits<int8_t>::max(),
               std::numeric_limits<int8_t>::min(),
               std::numeric_limits<int16_t>::max(),
               std::numeric_limits<int16_t>::min()}),
          makeFlatVector<int32_t>(
              {123,
               std::numeric_limits<int16_t>::max(),
               std::numeric_limits<int16_t>::min(),
               std::numeric_limits<int32_t>::max(),
               std::numeric_limits<int32_t>::min()}),
          makeFlatVector<int32_t>(
              {123,
               std::numeric_limits<int8_t>::max(),
               std::numeric_limits<int8_t>::min(),
               std::numeric_limits<int32_t>::max(),
               std::numeric_limits<int32_t>::min()}),
      });

  RowVectorPtr smallerIntVectors = makeRowVector(
      {"c1", "c2", "c3"},
      {
          makeFlatVector<int8_t>({
              123,
              std::numeric_limits<int8_t>::max(),
              std::numeric_limits<int8_t>::min(),
              -1,
              0,
          }),
          makeFlatVector<int16_t>({
              123,
              std::numeric_limits<int16_t>::max(),
              std::numeric_limits<int16_t>::min(),
              -1,
              0,
          }),
          makeFlatVector<int8_t>({
              123,
              std::numeric_limits<int8_t>::max(),
              std::numeric_limits<int8_t>::min(),
              -1,
              0,
          }),
      });

  auto dataFile = TempFilePath::create();
  ParquetWriterOptions options;
  writeToParquetFile(dataFile->getPath(), {intVectors}, options);

  auto rowType = ROW({"c1", "c2", "c3"}, {TINYINT(), SMALLINT(), TINYINT()});
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .dataColumns(rowType)
                .endTableScan()
                .planNode();

  auto split = makeSplit(dataFile->getPath());
  auto result = AssertQueryBuilder(op)
                    .connectorSessionProperty(
                        kHiveConnectorId,
                        parquetSessionProperty(
                            ParquetConfig::kAllowInt32NarrowingSession),
                        "true")
                    .split(split)
                    .copyResults(pool());
  auto rows = result->as<RowVector>();

  assertEqualVectors(smallerIntVectors->childAt(0), rows->childAt(0));
  assertEqualVectors(smallerIntVectors->childAt(1), rows->childAt(1));
  assertEqualVectors(smallerIntVectors->childAt(2), rows->childAt(2));
}

TEST_F(ParquetTableScanTest, shortAndLongDecimalReadWithLargerPrecision) {
  // decimal.parquet holds two columns (a: DECIMAL(5, 2), b: DECIMAL(20, 5)) and
  // 20 rows (10 rows per group). Data is in plain uncompressed format:
  //   a: [100.01 .. 100.20]
  //   b: [100000000000000.00001 .. 100000000000000.00020]
  // This test reads the DECIMAL(5, 2)a and DECIMAL(20, 5) file columns
  // with DECIMAL(8, 2) and DECIMAL(22, 5) row types.
  vector_size_t kSize = 20;
  std::vector<int64_t> unscaledShortValues(kSize);
  std::iota(unscaledShortValues.begin(), unscaledShortValues.end(), 10001);
  std::vector<int128_t> longDecimalValues;
  for (int i = 1; i <= kSize; ++i) {
    if (i < 10) {
      longDecimalValues.emplace_back(
          HugeInt::parse(fmt::format("1000000000000000000{}", i)));
    } else {
      longDecimalValues.emplace_back(
          HugeInt::parse(fmt::format("100000000000000000{}", i)));
    }
  }

  RowVectorPtr expectedDecimalVectors = makeRowVector(
      {"c1", "c2"},
      {makeFlatVector<int64_t>(unscaledShortValues, DECIMAL(8, 2)),
       makeFlatVector<int128_t>(longDecimalValues, DECIMAL(22, 5))});

  const std::shared_ptr<TempDirectoryPath> dataFileFolder =
      TempDirectoryPath::create();
  auto filePath = getExampleFilePath("decimal.parquet");

  auto rowType = ROW({"c1", "c2"}, {DECIMAL(8, 2), DECIMAL(22, 5)});
  auto op = PlanBuilder()
                .startTableScan()
                .outputType(rowType)
                .dataColumns(rowType)
                .endTableScan()
                .planNode();

  auto split = makeSplit(filePath);
  auto result = AssertQueryBuilder(op).split(split).copyResults(pool());
  auto rows = result->as<RowVector>();

  assertEqualVectors(expectedDecimalVectors->childAt(0), rows->childAt(0));
  assertEqualVectors(expectedDecimalVectors->childAt(1), rows->childAt(1));
}

TEST_F(ParquetTableScanTest, inFilter) {
  auto vectors = {makeRowVector(
      {"name"},
      {
          makeNullableFlatVector<std::string>(
              {"mary", "martin", "lucy", "alex", std::nullopt, "mary", "dan"}),
      })};
  auto filePath = TempFilePath::create();
  ParquetWriterOptions options;
  writeToParquetFile(filePath->getPath(), vectors, options);
  createDuckDbTable(vectors);

  // Test in.
  auto plan = PlanBuilder(pool_.get())
                  .tableScan(
                      ROW({"name"}, {VARCHAR()}),
                      {"name in ('alex', 'leo', 'mary', null, 'victor')"},
                      "")
                  .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .split(makeSplit(filePath->getPath()))
      .assertResults(
          "SELECT name FROM tmp where name in ('alex', 'leo', 'mary', null, 'victor')");

  // Test not in.
  plan = PlanBuilder(pool_.get())
             .tableScan(
                 ROW({"name"}, {VARCHAR()}),
                 {"name not in ('alex', 'leo', 'mary', null, 'victor')"},
                 "")
             .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .split(makeSplit(filePath->getPath()))
      .assertResults(
          "SELECT name FROM tmp where name not in ('alex', 'leo', 'mary', null, 'victor')");
}

TEST_F(ParquetTableScanTest, longDecimalDynamicFilterPushdown) {
  const auto decimalType = DECIMAL(38, 2);
  constexpr uint64_t kLowBits = 42;
  auto keyAt = [](int32_t keyIndex) {
    return HugeInt::build(/*hi=*/keyIndex + 1, kLowBits);
  };

  constexpr vector_size_t kNumRows = 20;
  std::vector<int128_t> factKeys;
  std::vector<int64_t> payloads;
  factKeys.reserve(kNumRows);
  payloads.reserve(kNumRows);
  for (auto row = 0; row < kNumRows; ++row) {
    factKeys.push_back(keyAt(row % 10));
    payloads.push_back(row);
  }

  const std::vector<int32_t> buildKeyIndexes = {1, 3, 6, 9};
  std::vector<int128_t> buildKeys;
  std::vector<int64_t> buildPayloads;
  buildKeys.reserve(buildKeyIndexes.size());
  buildPayloads.reserve(buildKeyIndexes.size());
  for (const auto keyIndex : buildKeyIndexes) {
    buildKeys.push_back(keyAt(keyIndex));
    buildPayloads.push_back(100 + keyIndex);
  }
  auto containsBuildKeyIndex = [&](int32_t keyIndex) {
    return std::find(
               buildKeyIndexes.begin(), buildKeyIndexes.end(), keyIndex) !=
        buildKeyIndexes.end();
  };

  auto fact = makeRowVector(
      {"k", "payload"},
      {
          makeFlatVector<int128_t>(factKeys, decimalType),
          makeFlatVector<int64_t>(payloads),
      });
  auto build = makeRowVector(
      {"dk", "dim_payload"},
      {
          makeFlatVector<int128_t>(buildKeys, decimalType),
          makeFlatVector<int64_t>(buildPayloads),
      });

  std::vector<int128_t> expectedKeys;
  std::vector<int64_t> expectedPayloads;
  std::vector<int64_t> expectedBuildPayloads;
  for (auto row = 0; row < kNumRows; ++row) {
    const auto keyIndex = row % 10;
    if (containsBuildKeyIndex(keyIndex)) {
      expectedKeys.push_back(factKeys[row]);
      expectedPayloads.push_back(payloads[row]);
      expectedBuildPayloads.push_back(100 + keyIndex);
    }
  }
  auto expected = makeRowVector(
      {"k", "payload", "dim_payload"},
      {
          makeFlatVector<int128_t>(expectedKeys, decimalType),
          makeFlatVector<int64_t>(expectedPayloads),
          makeFlatVector<int64_t>(expectedBuildPayloads),
      });

  auto filePath = TempFilePath::create();
  ParquetWriterOptions options;
  writeToParquetFile(filePath->getPath(), {fact}, options);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId joinId;
  auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(asRowType(fact->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"k"},
                      {"dk"},
                      PlanBuilder(planNodeIdGenerator, pool_.get())
                          .values({build})
                          .planNode(),
                      "",
                      {"k", "payload", "dim_payload"})
                  .capturePlanNodeId(joinId)
                  .planNode();

  auto task = AssertQueryBuilder(plan)
                  .split(probeScanId, makeSplit(filePath->getPath()))
                  .assertResults(expected);
  assertDynamicFilterProduced(task, probeScanId, joinId);
}

TEST_F(ParquetTableScanTest, longDecimalDynamicFilterReplacement) {
  const auto decimalType = DECIMAL(38, 2);
  constexpr uint64_t kLowBits = 17;
  auto keyAt = [](int32_t keyIndex) {
    return HugeInt::build(/*hi=*/keyIndex + 1, kLowBits);
  };

  constexpr vector_size_t kNumRows = 20;
  std::vector<int128_t> factKeys;
  factKeys.reserve(kNumRows);
  for (auto row = 0; row < kNumRows; ++row) {
    factKeys.push_back(keyAt(row % 10));
  }

  const std::vector<int32_t> buildKeyIndexes = {0, 2, 5, 8};
  std::vector<int128_t> buildKeys;
  buildKeys.reserve(buildKeyIndexes.size());
  for (const auto keyIndex : buildKeyIndexes) {
    buildKeys.push_back(keyAt(keyIndex));
  }
  auto containsBuildKeyIndex = [&](int32_t keyIndex) {
    return std::find(
               buildKeyIndexes.begin(), buildKeyIndexes.end(), keyIndex) !=
        buildKeyIndexes.end();
  };

  auto fact = makeRowVector(
      {"k"},
      {
          makeFlatVector<int128_t>(factKeys, decimalType),
      });
  auto build = makeRowVector(
      {"dk"},
      {
          makeFlatVector<int128_t>(buildKeys, decimalType),
      });

  std::vector<int128_t> expectedKeys;
  for (auto row = 0; row < kNumRows; ++row) {
    const auto keyIndex = row % 10;
    if (containsBuildKeyIndex(keyIndex)) {
      expectedKeys.push_back(factKeys[row]);
    }
  }
  auto expected = makeRowVector(
      {"k"},
      {
          makeFlatVector<int128_t>(expectedKeys, decimalType),
      });

  auto filePath = TempFilePath::create();
  ParquetWriterOptions options;
  writeToParquetFile(filePath->getPath(), {fact}, options);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId joinId;
  auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(asRowType(fact->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"k"},
                      {"dk"},
                      PlanBuilder(planNodeIdGenerator, pool_.get())
                          .values({build})
                          .planNode(),
                      "",
                      {"k"})
                  .capturePlanNodeId(joinId)
                  .planNode();

  auto task = AssertQueryBuilder(plan)
                  .split(probeScanId, makeSplit(filePath->getPath()))
                  .assertResults(expected);
  assertDynamicFilterProduced(task, probeScanId, joinId);

  const auto planStats = toPlanStats(task->taskStats());
  ASSERT_EQ(
      planStats.at(joinId).customStats.at("replacedWithDynamicFilterRows").sum,
      expectedKeys.size());
}

TEST_F(ParquetTableScanTest, reusedLazyVectors) {
  const std::vector<std::string> columnNames = {"a", "b"};
  std::vector<RowVectorPtr> data;
  for (auto row = 0; row < 10; ++row) {
    data.emplace_back(makeRowVector(
        columnNames,
        {
            makeFlatVector<int64_t>({static_cast<int64_t>(row % 5)}),
            makeFlatVector<int64_t>({static_cast<int64_t>(row)}),
        }));
  }
  const auto expectedRowVector = makeRowVector(
      {makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
       makeFlatVector<int64_t>({5, 7, 9, 11, 13}),
       makeFlatVector<int64_t>({5, 7, 9, 11, 13})});

  const auto filePath = TempFilePath::create();
  ParquetWriterOptions options;
  writeToParquetFile(filePath->getPath(), data, options);

  const auto plan = PlanBuilder()
                        .tableScan(ROW(columnNames, {BIGINT(), BIGINT()}))
                        .project({"a as c1", "b as c2", "b as c3"})
                        .singleAggregation({"c1"}, {"sum(c2)", "sum(c3)"})
                        .planNode();
  AssertQueryBuilder(plan)
      .split(makeSplit(filePath->getPath()))
      .assertResults(expectedRowVector);
}

// Verify that entire Parquet files are pruned based on file-level column
// statistics when the filter eliminates all data in the file.
TEST_F(ParquetTableScanTest, statsBasedFileSkipping) {
  ParquetWriterOptions options;
  std::vector<std::string> filePaths;
  std::vector<RowVectorPtr> dataVectors;
  const vector_size_t numRows = 100;
  filePaths.push_back(TempFilePath::create()->getPath());
  dataVectors.push_back(makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<double>(
              numRows, [](auto row) { return static_cast<double>(row); }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                static std::vector<StringView> values = {"a", "b", "c", "d"};
                return values[row % values.size()];
              }),
      }));
  // File 1: integers [0, 99], doubles [0.0, 99.0], strings ["a".."d"].
  writeToParquetFile(filePaths.back(), dataVectors, options);

  filePaths.push_back(TempFilePath::create()->getPath());
  dataVectors.push_back(makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row + 200; }),
          makeFlatVector<double>(numRows, [](auto row) { return row + 200; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                static std::vector<StringView> values = {"p", "q", "r", "s"};
                return values[row % values.size()];
              }),
      }));
  // File 2: integers [200, 299], doubles [200.0, 299.0], strings ["p".."s"].
  writeToParquetFile(filePaths.back(), {dataVectors.back()}, options);

  createDuckDbTable(dataVectors);

  auto makeSplits = [&]() {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    for (const auto& path : filePaths) {
      splits.push_back(makeSplit(path));
    }
    return splits;
  };

  auto testFileSkipping = [&](const std::string& filter,
                              int32_t expectedSkipped,
                              int32_t expectedProcessed) {
    SCOPED_TRACE(filter);
    auto plan = PlanBuilder(pool_.get())
                    .tableScan(dataVectors.back()->rowType(), {filter})
                    .planNode();
    auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                    .splits(makeSplits())
                    .assertResults("SELECT * FROM tmp WHERE " + filter);
    auto stats =
        task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
    EXPECT_EQ(stats["skippedSplits"].sum, expectedSkipped);
    EXPECT_EQ(stats["processedSplits"].sum, expectedProcessed);
  };

  // Neither file has values > 1000, both files skipped.
  testFileSkipping("c0 > 1000", 2, 0);
  // Neither file has values < 0, both files skipped.
  testFileSkipping("c0 < 0", 2, 0);
  // Low-range file (max=99) is skipped, high-range file is read.
  testFileSkipping("c0 >= 200", 1, 1);
  // High-range file (min=200) is skipped, low-range file is read.
  testFileSkipping("c0 <= 99", 1, 1);
  // Double column: both files skipped.
  testFileSkipping("c1 > 500.0", 2, 0);
  // String column: low-range has ["a".."d"], high-range has ["p".."s"], both
  // skipped.
  testFileSkipping("c2 = 'z'", 2, 0);
  // Matches both files, no files skipped.
  testFileSkipping("c0 >= 0", 0, 2);
}

TEST_F(ParquetTableScanTest, fileFormatRuntimeStats) {
  auto rowType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto vector = makeRowVector(
      {"a", "b"},
      {
          makeFlatVector<int64_t>(100, [](auto row) { return row; }),
          makeFlatVector<double>(100, [](auto row) { return row * 1.5; }),
      });
  std::vector<RowVectorPtr> vectors = {vector};

  // Write one Parquet file and one DWRF file.
  auto parquetFile = TempFilePath::create();
  ParquetWriterOptions parquetOptions;
  writeToParquetFile(parquetFile->getPath(), vectors, parquetOptions);

  auto dwrfFile = TempFilePath::create();
  writeToFile(dwrfFile->getPath(), vectors);

  // DuckDB reference table with data from both files.
  std::vector<RowVectorPtr> allVectors = {vector, vector};
  createDuckDbTable(allVectors);

  auto parquetSplit = makeHiveConnectorSplits(
      parquetFile->getPath(), 1, dwio::common::FileFormat::PARQUET);
  auto dwrfSplit = makeHiveConnectorSplits(
      dwrfFile->getPath(), 1, dwio::common::FileFormat::DWRF);

  auto plan = PlanBuilder().tableScan(asRowType(vectors[0]->type())).planNode();

  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .splits({parquetSplit[0], dwrfSplit[0]})
                  .assertResults("SELECT * FROM tmp");

  auto stats = task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
  ASSERT_EQ(stats.count("fileFormat.parquet"), 1);
  ASSERT_EQ(stats.at("fileFormat.parquet").sum, 1);
  ASSERT_EQ(stats.count("fileFormat.dwrf"), 1);
  ASSERT_EQ(stats.at("fileFormat.dwrf").sum, 1);

  task.reset();
  waitForAllTasksToBeDeleted();
}

TEST_F(ParquetTableScanTest, structSkipNulls) {
  constexpr vector_size_t kNumRows = 500;

  auto id = makeFlatVector<int64_t>(
      kNumRows, [](auto row) { return static_cast<int64_t>(row); });
  // A struct that is null for every row -- read for its null-ness only.
  auto structColumn = makeRowVector(
      {"a", "b"},
      {
          makeFlatVector<int64_t>(kNumRows, [](auto row) { return row; }),
          makeFlatVector<int64_t>(kNumRows, [](auto row) { return row; }),
      },
      [](vector_size_t /*row*/) { return true; });

  auto data = makeRowVector({"id", "s"}, {id, structColumn});
  auto dataType = asRowType(data->type());

  auto filePath = TempFilePath::create();
  ParquetWriterOptions options;
  writeToParquetFile(filePath->getPath(), {data}, options);
  loadData(dataType, data);

  parse::ParseOptions parseOptions;
  parseOptions.parseDecimalAsDouble = false;
  auto plan = PlanBuilder(pool_.get())
                  .setParseOptions(parseOptions)
                  .tableScan(
                      dataType,
                      {"id >= 200", "s IS NULL"},
                      /*remainingFilter=*/"",
                      /*dataColumns=*/nullptr,
                      /*assignments=*/{})
                  .planNode();

  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits = {
      makeSplit(filePath->getPath())};

  // Small read batch: the leading rows (id < 200) fill whole batches that
  // 'id >= 200' filters out entirely, so the nulls-only 's' column lags and is
  // then skipped forward.
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchRows, "64")
      .config(core::QueryConfig::kMaxOutputBatchRows, "64")
      .splits(splits)
      .assertResults("SELECT id, s FROM tmp WHERE id >= 200 AND s IS NULL");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
