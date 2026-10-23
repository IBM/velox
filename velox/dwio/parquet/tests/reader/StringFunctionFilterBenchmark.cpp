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

// Measures table scans with remaining filters on string functions, e.g.
// lower(c) = 'aws rds', with and without deriving subfield filters from them.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/SharedArbitrator.h"
#include "velox/dwio/parquet/tests/reader/ParquetTableScanTestBase.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_int32(num_rows, 2'000'000, "Number of rows in each file.");

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {

// Baseline parser. Rejects predicates whose first argument is a function call,
// e.g. lower(c) = 'x', length(c) < 5 or strpos(c, 's') > 0, and derives no
// implied filters, so such predicates stay in the remaining filter. It still
// pushes down c LIKE '%s%', which no benchmark case uses.
class BaselineParser : public exec::PrestoExprToSubfieldFilterParser {
 public:
  std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
  leafCallToSubfieldFilter(
      const core::CallTypedExpr& call,
      core::ExpressionEvaluator* evaluator,
      bool negated) override {
    if (!call.inputs().empty() && call.inputs()[0]->isCallKind()) {
      return std::nullopt;
    }
    return PrestoExprToSubfieldFilterParser::leafCallToSubfieldFilter(
        call, evaluator, negated);
  }

  std::optional<std::pair<common::Subfield, std::unique_ptr<common::Filter>>>
  leafCallToImpliedSubfieldFilter(
      const core::CallTypedExpr& /*call*/,
      core::ExpressionEvaluator* /*evaluator*/,
      bool /*negated*/) override {
    return std::nullopt;
  }
};

class StringFunctionFilterBenchmark
    : public parquet::test::ParquetTableScanTestBase {
 public:
  StringFunctionFilterBenchmark() {
    ParquetTableScanTestBase::SetUp();

    // 'service' has few distinct values and is dictionary encoded unless
    // dictionaries are disabled. 'resource' has mostly distinct values, so
    // the writer falls back to plain encoding.
    const std::vector<std::string> services = {
        "AWS RDS",
        "Amazon Elastic Compute Cloud",
        "Amazon Simple Storage Service",
        "AWS Lambda",
        "Amazon DynamoDB",
        "Amazon CloudFront",
        "AWS Key Management Service",
        "Amazon Virtual Private Cloud",
        "Amazon Elastic Container Service",
        "AWS Data Transfer",
        "Amazon Redshift",
        "AWS Glue",
    };
    auto data = makeRowVector(
        {"service", "resource", "cost"},
        {
            makeFlatVector<std::string>(
                FLAGS_num_rows,
                [&](auto row) { return services[row % services.size()]; }),
            makeFlatVector<std::string>(
                FLAGS_num_rows,
                [](auto row) {
                  return row % 20 == 0
                      ? std::string("(not set)")
                      : fmt::format(
                            "arn:aws:rds:us-east-1:123456789012:db:Instance-{}",
                            row);
                }),
            makeFlatVector<double>(
                FLAGS_num_rows, [](auto row) { return row * 0.01; }),
        });
    rowType_ = asRowType(data->type());

    dictionaryFile_ = common::testutil::TempFilePath::create();
    writeToParquetFile(dictionaryFile_->getPath(), {data});
    plainFile_ = common::testutil::TempFilePath::create();
    parquet::ParquetWriterOptions plainOptions;
    plainOptions.enableDictionary = false;
    writeToParquetFile(plainFile_->getPath(), {data}, plainOptions);

    // Sorted by 'resource', in row groups of kRowsPerRowGroup rows with pages
    // of kRowsPerPage rows and a page index. Each row group holds two
    // services, so a service appears in a sixth of the row groups.
    auto sortedData = makeRowVector(
        {"service", "resource", "cost"},
        {
            makeFlatVector<std::string>(
                FLAGS_num_rows,
                [&](auto row) {
                  const auto rowGroup = row / kRowsPerRowGroup;
                  // Odd rows take a service from the other half of the list.
                  const auto offset = (row % 2) * (services.size() / 2);
                  return services[(rowGroup + offset) % services.size()];
                }),
            makeFlatVector<std::string>(
                FLAGS_num_rows,
                [](auto row) {
                  return fmt::format(
                      "arn:aws:rds:us-east-1:123456789012:db:Instance-{:08}",
                      row);
                }),
            makeFlatVector<double>(
                FLAGS_num_rows, [](auto row) { return row * 0.01; }),
        });
    sortedFile_ = common::testutil::TempFilePath::create();
    dwio::common::WriterOptions sortedWriterOptions;
    sortedWriterOptions.flushPolicyFactory = []() {
      return std::make_unique<parquet::DefaultFlushPolicy>(
          kRowsPerRowGroup, std::numeric_limits<int64_t>::max());
    };
    parquet::ParquetWriterOptions sortedOptions;
    sortedOptions.enableWritePageIndex = true;
    // Each write batch ends a page, because it exceeds the 1-byte page size.
    sortedOptions.dataPageSize = 1;
    sortedOptions.batchSize = kRowsPerPage;
    writeToParquetFile(
        sortedFile_->getPath(),
        {sortedData},
        std::move(sortedWriterOptions),
        std::move(sortedOptions));
  }

  ~StringFunctionFilterBenchmark() override {
    // Leaves the default parser registered after a baseline run.
    exec::ExprToSubfieldFilterParser::registerParser(
        std::make_shared<exec::PrestoExprToSubfieldFilterParser>());
    ParquetTableScanTestBase::TearDown();
  }

  void TestBody() override {}

  void addBenchmarks() {
    const std::vector<std::pair<std::string, std::string>> filters = {
        {"lower_eq", "lower(service) = 'aws rds'"},
        {"lower_neq", "lower(resource) <> '(not set)'"},
        {"lower_eq_and_neq",
         "lower(service) = 'aws rds' and lower(resource) <> '(not set)'"},
        {"lower_in", "lower(service) in ('aws lambda', 'aws glue')"},
        {"lower_or",
         "lower(service) = 'aws lambda' or lower(service) = 'aws glue'"},
        {"like", "resource like '(not _et)'"},
        {"strpos_lower", "strpos(lower(service), 'lambda') > 0"},
        {"strpos_lower_resource",
         "strpos(lower(resource), 'instance-1999') > 0"},
        {"length", "length(resource) < 50"},
    };
    for (const auto& [name, filter] : filters) {
      addComparison(
          fmt::format("{}_dictionary", name),
          filter,
          dictionaryFile_,
          kReaderSkippingOnly,
          kAll);
      addComparison(
          fmt::format("{}_plain", name),
          filter,
          plainFile_,
          kReaderSkippingOnly,
          kAll);
      folly::addBenchmark(__FILE__, "-", []() { return 0; });
    }

    // Each skipping step compared with the same scan without it. The filter
    // selects a row in the middle of the file, away from a page boundary.
    const auto targetRow = FLAGS_num_rows / 2 + 123;
    const auto resourceFilter = fmt::format(
        "lower(resource) = 'arn:aws:rds:us-east-1:123456789012:db:instance-{:08}'",
        targetRow);
    // The baseline keeps lower() in the remaining filter, so this includes the
    // gain from pushing it down.
    addComparison(
        "skip_row_groups_by_min_max",
        resourceFilter,
        sortedFile_,
        kNone,
        kPushdownOnly);
    addComparison(
        "skip_row_groups_by_dictionary",
        "lower(service) = 'aws rds'",
        sortedFile_,
        kPushdownOnly,
        kPushdownAndDictionary);
    addComparison(
        "skip_pages_by_page_index",
        resourceFilter,
        sortedFile_,
        kPushdownOnly,
        kPushdownAndPageIndex);
  }

 private:
  static constexpr int32_t kRowsPerRowGroup = 100'000;
  static constexpr int32_t kRowsPerPage = 1'000;

  // Optimizations to enable in a run.
  struct Options {
    // Pushes down filters on lower(), upper(), length(), strpos() and LIKE.
    bool pushdown;
    // Skips row groups whose dictionary has no passing value.
    bool dictionaryFilter;
    // Skips pages by the page index.
    bool pageIndexFilter;
  };

  static constexpr Options kNone{
      .pushdown = false,
      .dictionaryFilter = false,
      .pageIndexFilter = false};
  static constexpr Options kReaderSkippingOnly{
      .pushdown = false,
      .dictionaryFilter = true,
      .pageIndexFilter = true};
  static constexpr Options kPushdownOnly{
      .pushdown = true,
      .dictionaryFilter = false,
      .pageIndexFilter = false};
  static constexpr Options kPushdownAndDictionary{
      .pushdown = true,
      .dictionaryFilter = true,
      .pageIndexFilter = false};
  static constexpr Options kPushdownAndPageIndex{
      .pushdown = true,
      .dictionaryFilter = false,
      .pageIndexFilter = true};
  static constexpr Options kAll{
      .pushdown = true,
      .dictionaryFilter = true,
      .pageIndexFilter = true};

  // Adds a benchmark of scanning 'file' with 'filter' using 'baseline' and a
  // relative one using 'options'.
  void addComparison(
      const std::string& name,
      const std::string& filter,
      const std::shared_ptr<common::testutil::TempFilePath>& file,
      Options baseline,
      Options options) {
    folly::addBenchmark(__FILE__, name, [this, filter, file, baseline]() {
      run(filter, file->getPath(), baseline);
      return 1;
    });
    folly::addBenchmark(__FILE__, "%" + name, [this, filter, file, options]() {
      run(filter, file->getPath(), options);
      return 1;
    });
  }

  void run(
      const std::string& filter,
      const std::string& path,
      const Options& options) {
    folly::BenchmarkSuspender suspender;
    if (options.pushdown) {
      exec::ExprToSubfieldFilterParser::registerParser(
          std::make_shared<exec::PrestoExprToSubfieldFilterParser>());
    } else {
      exec::ExprToSubfieldFilterParser::registerParser(
          std::make_shared<BaselineParser>());
    }
    auto plan = PlanBuilder(pool())
                    .tableScan(rowType_, {}, filter)
                    .singleAggregation({}, {"count(1)", "sum(cost)"})
                    .planNode();
    suspender.dismiss();
    auto result = AssertQueryBuilder(plan)
                      .connectorSessionProperty(
                          exec::test::kHiveConnectorId,
                          parquetSessionProperty("dictionary_filter_enabled"),
                          options.dictionaryFilter ? "true" : "false")
                      .connectorSessionProperty(
                          exec::test::kHiveConnectorId,
                          parquetSessionProperty("page_index_filter_enabled"),
                          options.pageIndexFilter ? "true" : "false")
                      .split(makeSplit(path))
                      .copyResults(pool());
    folly::doNotOptimizeAway(result);
  }

  RowTypePtr rowType_;
  std::shared_ptr<common::testutil::TempFilePath> dictionaryFile_;
  std::shared_ptr<common::testutil::TempFilePath> plainFile_;
  std::shared_ptr<common::testutil::TempFilePath> sortedFile_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  memory::SharedArbitrator::registerFactory();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();

  StringFunctionFilterBenchmark benchmark;
  benchmark.addBenchmarks();
  folly::runBenchmarks();
  return 0;
}
