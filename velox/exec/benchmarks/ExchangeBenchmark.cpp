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
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/core/QueryConfig.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_int32(task_width, 4, "Number of threads in each task in shuffle");

DEFINE_int64(exchange_buffer_mb, 32, "task-wide buffer in remote exchange");
DEFINE_int32(
    dict_pct,
    0,
    "Percentage of vectors per column wrapped in dictionary encoding. "
    "Applied independently to each column across all generated row vectors "
    "and recursively to nested children.");
// Add the following definitions to allow Clion runs
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

/// Benchmarks repartition/exchange with different batch sizes,
/// numbers of destinations and data type mixes.  Generates a plan
/// that 1. shuffles a constant input in each of n workers, sending
/// each partition to n consumers in the next stage. The consumers
/// count the rows and send the count to a final single task stage
/// that returns the sum of the counts. The sum is expected to be
/// n * number of rows in constant input.

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

bool shouldWrapVector(
    int32_t vectorIndex,
    int32_t numVectors,
    int32_t dictPct) {
  VELOX_CHECK_GE(dictPct, 0);
  VELOX_CHECK_LE(dictPct, 100);
  return dictPct > 0 && (vectorIndex * 100) / numVectors < dictPct;
}

void wrapDictionaryRecursive(VectorPtr& vector) {
  if (!vector) {
    return;
  }

  switch (vector->encoding()) {
    case VectorEncoding::Simple::ROW: {
      auto row = vector->as<RowVector>();
      for (auto i = 0; i < row->childrenSize(); ++i) {
        wrapDictionaryRecursive(row->childAt(i));
      }
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto array = vector->as<ArrayVector>();
      auto elements = array->elements();
      wrapDictionaryRecursive(elements);
      array->setElements(std::move(elements));
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto map = vector->as<MapVector>();
      auto keys = map->mapKeys();
      auto values = map->mapValues();
      wrapDictionaryRecursive(keys);
      wrapDictionaryRecursive(values);
      map->setKeysAndValues(std::move(keys), std::move(values));
      break;
    }
    default:
      break;
  }

  auto indices = facebook::velox::test::makeIndices(
      vector->size(), [](auto row) { return row; }, vector->pool());
  vector =
      BaseVector::wrapInDictionary(nullptr, indices, vector->size(), vector);
}

struct ExchangeRunStats {
  int64_t wallUs = 0;
  PlanNodeStats partitionedOutputStats;
  PlanNodeStats exchangeStats;
};

enum class ExchangeMode {
  kNormal,
  kOptimized,
};

/// Column element type dimension for simple-schema exchange benchmarks.
enum class SimpleColType {
  kBoolean,
  kTinyint,
  kInteger,
  kBigint,
  kHugeint,
  kLongDecimal,
  kDouble,
  kVarchar,
};

TypePtr simpleColTypeToType(SimpleColType colType) {
  switch (colType) {
    case SimpleColType::kBoolean:
      return BOOLEAN();
    case SimpleColType::kTinyint:
      return TINYINT();
    case SimpleColType::kInteger:
      return INTEGER();
    case SimpleColType::kBigint:
      return BIGINT();
    case SimpleColType::kHugeint:
      return HUGEINT();
    case SimpleColType::kLongDecimal:
      return DECIMAL(20, 3);
    case SimpleColType::kDouble:
      return DOUBLE();
    case SimpleColType::kVarchar:
      return VARCHAR();
  }
  VELOX_UNREACHABLE();
}

std::string simpleColTypeName(SimpleColType colType) {
  switch (colType) {
    case SimpleColType::kBoolean:
      return "Boolean";
    case SimpleColType::kTinyint:
      return "Tinyint";
    case SimpleColType::kInteger:
      return "Integer";
    case SimpleColType::kBigint:
      return "Bigint";
    case SimpleColType::kHugeint:
      return "Hugeint";
    case SimpleColType::kLongDecimal:
      return "LongDecimal";
    case SimpleColType::kDouble:
      return "Double";
    case SimpleColType::kVarchar:
      return "Varchar";
  }
  VELOX_UNREACHABLE();
}

enum class ExchangeInputKind {
  kDeep10K,
  kDeep50,
  kStruct1K,
  kNested1_1K,
  kNested2_1K,
  kNested3_1K,
};

struct ExchangeInputSpec {
  std::string name;
  RowTypePtr type;
  int32_t numVectors;
  int32_t rowsPerVector;
};

struct ExchangeBenchmarkResult {
  std::string datasetName;
  ExchangeMode mode;
  ExchangeRunStats stats;
};

std::vector<ExchangeBenchmarkResult> benchmarkResults;

std::string modeName(ExchangeMode mode) {
  switch (mode) {
    case ExchangeMode::kNormal:
      return "normal";
    case ExchangeMode::kOptimized:
      return "optimized";
  }

  VELOX_UNREACHABLE();
}

/// Creates a simple row type with `numCols` columns all of type `colType`.
RowTypePtr makeSimpleType(const TypePtr& colType, int32_t numCols) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(numCols);
  types.reserve(numCols);
  for (int32_t i = 0; i < numCols; ++i) {
    names.push_back(fmt::format("c{}", i));
    types.push_back(colType);
  }
  return ROW(std::move(names), std::move(types));
}

RowTypePtr makeStructType() {
  return ROW(
      {{"c0", BIGINT()},
       {"r1",
        ROW(
            {{"k2", BIGINT()},
             {"r2",
              ROW(
                  {{"i1", BIGINT()},
                   {"i2", BIGINT()},
                   {"r3",
                    ROW(
                        {{"s3", VARCHAR()},
                         {"i5", INTEGER()},
                         {"d5", DOUBLE()},
                         {"b5", BOOLEAN()},
                         {"a5", ARRAY(TINYINT())}})}})}})}});
}

RowTypePtr makeDeepType() {
  return ROW(
      {{"c0", BIGINT()},
       {"long_array_val", ARRAY(ARRAY(BIGINT()))},
       {"array_val", ARRAY(VARCHAR())},
       {"struct_val", ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
       {"map_val",
        MAP(VARCHAR(),
            MAP(BIGINT(),
                ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))}});
}

// N levels of single-field ROW wrapping a BIGINT leaf, behind a BIGINT "c0"
// partition key. Exercises the nested-ROW serialization path; combined with
// nullPct it produces nulls at every ROW level and the leaf, on disjoint sets
// of rows.
RowTypePtr makeNestedType(int level) {
  TypePtr inner = BIGINT();
  for (int i = 0; i < level; ++i) {
    inner = ROW({"r"}, {inner});
  }
  return ROW({"c0", "v"}, {BIGINT(), inner});
}

ExchangeInputSpec makeInputSpec(ExchangeInputKind kind) {
  switch (kind) {
    case ExchangeInputKind::kDeep10K:
      return {"Deep10K", makeDeepType(), 10, 10000};
    case ExchangeInputKind::kDeep50:
      return {"Deep50", makeDeepType(), 2000, 50};
    case ExchangeInputKind::kStruct1K:
      return {"Struct1K", makeStructType(), 100, 1000};
    case ExchangeInputKind::kNested1_1K:
      return {"Nested1_1K", makeNestedType(1), 100, 1000};
    case ExchangeInputKind::kNested2_1K:
      return {"Nested2_1K", makeNestedType(2), 100, 1000};
    case ExchangeInputKind::kNested3_1K:
      return {"Nested3_1K", makeNestedType(3), 100, 1000};
  }

  VELOX_UNREACHABLE();
}

ExchangeInputSpec makeInputSpec(SimpleColType colType, int32_t numCols) {
  return {
      fmt::format("10K_{}_col{}", simpleColTypeName(colType), numCols),
      makeSimpleType(simpleColTypeToType(colType), numCols),
      10,
      10'000};
}

std::string formatStat(const ExchangeRunStats* stats, auto formatter) {
  if (stats == nullptr) {
    return "N/A";
  }
  return formatter(*stats);
}

void printAllExchangeStats() {
  struct PairedStats {
    const ExchangeRunStats* normal = nullptr;
    const ExchangeRunStats* optimized = nullptr;
  };

  std::vector<std::string> datasetOrder;
  std::unordered_map<std::string, PairedStats> groupedStats;
  for (const auto& result : benchmarkResults) {
    auto [it, inserted] =
        groupedStats.try_emplace(result.datasetName, PairedStats{});
    if (inserted) {
      datasetOrder.push_back(result.datasetName);
    }
    if (result.mode == ExchangeMode::kNormal) {
      it->second.normal = &result.stats;
    } else {
      it->second.optimized = &result.stats;
    }
  }

  for (const auto& datasetName : datasetOrder) {
    const auto statsIt = groupedStats.find(datasetName);
    VELOX_CHECK(statsIt != groupedStats.end());
    const auto& paired = statsIt->second;
    std::cout << "--------------------" << datasetName << "--------------------"
              << std::endl;
    std::cout << "Wall Time (ms) | normal: "
              << formatStat(
                     paired.normal,
                     [](const ExchangeRunStats& stats) {
                       return succinctMicros(stats.wallUs);
                     })
              << " | optimized: "
              << formatStat(
                     paired.optimized,
                     [](const ExchangeRunStats& stats) {
                       return succinctMicros(stats.wallUs);
                     })
              << std::endl;
    std::cout << "Normal" << std::endl
              << " - PartitionedOutput: "
              << formatStat(
                     paired.normal,
                     [](const ExchangeRunStats& stats) {
                       return stats.partitionedOutputStats.toString();
                     })
              << std::endl
              << " - Exchange: "
              << formatStat(
                     paired.normal,
                     [](const ExchangeRunStats& stats) {
                       return stats.exchangeStats.toString();
                     })
              << std::endl;
    std::cout << "Optimized" << std::endl
              << " - PartitionedOutput: "
              << formatStat(
                     paired.optimized,
                     [](const ExchangeRunStats& stats) {
                       return stats.partitionedOutputStats.toString();
                     })
              << std::endl
              << " - Exchange: "
              << formatStat(
                     paired.optimized,
                     [](const ExchangeRunStats& stats) {
                       return stats.exchangeStats.toString();
                     })
              << std::endl;
  }
}

template <typename Fn>
ExchangeRunStats runBenchmarkIterations(unsigned int iters, Fn&& runOnce) {
  ExchangeRunStats stats;
  while (iters--) {
    stats = runOnce();
  }
  return stats;
}

class ExchangeBenchmark : public VectorTestBase {
 public:
  /// Creates a single flat column of `type` with `numRows` rows.
  /// Approximately `nullPct` percent of rows are set to null, distributed
  /// uniformly over a window of 100 rows starting at `nullPhase`
  /// ((row + nullPhase) % 100 < nullPct). Non-null values are sequential
  /// integers cast to the native type. VARCHAR values alternate between inline
  /// and non-inline strings, and `vectorIndex` helps constant columns cover
  /// both cases.
  VectorPtr makeColumn(
      const TypePtr& type,
      int32_t numRows,
      int32_t nullPct,
      int32_t vectorIndex = 0,
      int32_t nullPhase = 0) {
    std::function<bool(vector_size_t)> isNull;
    if (nullPct == 100) {
      isNull = [](auto) { return true; };
    } else if (nullPct > 0) {
      isNull = [nullPct, nullPhase](vector_size_t row) {
        return ((row + nullPhase) % 100) < nullPct;
      };
    }

    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return makeFlatVector<bool>(
            numRows, [](auto row) { return row % 2 == 0; }, isNull);
      case TypeKind::TINYINT:
        return makeFlatVector<int8_t>(
            numRows, [](auto row) { return static_cast<int8_t>(row); }, isNull);
      case TypeKind::SMALLINT:
        return makeFlatVector<int16_t>(
            numRows,
            [](auto row) { return static_cast<int16_t>(row); },
            isNull);
      case TypeKind::INTEGER:
        return makeFlatVector<int32_t>(
            numRows, [](auto row) { return row; }, isNull);
      case TypeKind::BIGINT:
        // Handles plain BIGINT and short-decimal columns (DECIMAL(p,s), p≤18).
        return makeFlatVector<int64_t>(
            numRows,
            [](auto row) { return static_cast<int64_t>(row); },
            isNull,
            type);
      case TypeKind::REAL:
        return makeFlatVector<float>(
            numRows, [](auto row) { return static_cast<float>(row); }, isNull);
      case TypeKind::DOUBLE:
        return makeFlatVector<double>(
            numRows, [](auto row) { return static_cast<double>(row); }, isNull);
      case TypeKind::HUGEINT:
        // Handles long-decimal columns (DECIMAL(p,s), p>18).
        return makeFlatVector<int128_t>(
            numRows,
            [](auto row) { return static_cast<int128_t>(row); },
            isNull,
            type);
      case TypeKind::VARCHAR:
        return makeFlatVector<std::string>(
            numRows,
            [vectorIndex](auto row) {
              return std::string(
                  row % 2 == 0 ? StringView::kInlineSize - 2
                               : StringView::kInlineSize + 8,
                  static_cast<char>('a' + ((row + vectorIndex) % 26)));
            },
            isNull,
            type);
      case TypeKind::ROW: {
        // Build each child independently and apply row-level nulls so a ROW
        // column carries nulls at this level and, recursively, at every nested
        // ROW level and the leaf. Advancing the null window by nullPct per
        // level keeps the per-level null positions disjoint, so a null parent
        // really does drop rows its children would otherwise write. Sharing one
        // window across levels would make every mask identical and leave that
        // path unexercised.
        const auto& rowType = type->asRow();
        std::vector<std::string> names;
        std::vector<VectorPtr> children;
        names.reserve(rowType.size());
        children.reserve(rowType.size());
        for (auto i = 0; i < rowType.size(); ++i) {
          names.push_back(rowType.nameOf(i));
          children.push_back(makeColumn(
              rowType.childAt(i),
              numRows,
              nullPct,
              vectorIndex,
              nullPhase + nullPct));
        }
        return makeRowVector(names, children, isNull);
      }
      default:
        VELOX_NYI(
            "makeColumn does not support complex type {} yet",
            type->toString());
    }
  }

  VectorPtr makeConstantColumn(
      const TypePtr& type,
      int32_t numRows,
      int32_t nullPct,
      int32_t vectorIndex) {
    const auto baseNullPct =
        nullPct > 0 && (vectorIndex % 100) < nullPct ? 100 : 0;
    return BaseVector::wrapInConstant(
        numRows, 0, makeColumn(type, 1, baseNullPct, vectorIndex));
  }

  /// Generates input batches for the exchange benchmark.
  ///
  /// `dictPct` is the percentage of vectors for each column that should be
  /// wrapped in dictionary encoding across the full set of generated batches.
  /// `constantPct` works similarly, but creates ConstantVector columns.
  /// Constant vectors take precedence over dictionary wrapping.
  /// For example, with `numVectors = 10` and `dictPct = 30`, each top-level
  /// column will have 3 dictionary-encoded vectors and 7 simple vectors,
  /// unless those vectors are selected for constant encoding. Nested children
  /// of complex columns use the same rule recursively.
  ///
  /// `nullPct` controls what fraction of values in each column are null:
  /// 0 = no nulls, 50 = half the rows null, 100 = all rows null.
  std::vector<RowVectorPtr> makeRows(
      const RowTypePtr& type,
      int32_t numVectors,
      int32_t rowsPerVector,
      int32_t dictPct = 0,
      int32_t constantPct = 0,
      int32_t nullPct = 0) {
    std::vector<RowVectorPtr> vectors;
    vectors.reserve(numVectors);
    for (int32_t i = 0; i < numVectors; ++i) {
      const auto useConstant = shouldWrapVector(i, numVectors, constantPct);
      std::vector<VectorPtr> children;
      children.reserve(type->size());
      for (int32_t col = 0; col < type->size(); ++col) {
        if (useConstant) {
          children.push_back(makeConstantColumn(
              type->childAt(col), rowsPerVector, nullPct, i));
        } else {
          children.push_back(
              makeColumn(type->childAt(col), rowsPerVector, nullPct, i));
        }
      }
      auto vector = makeRowVector(type->names(), children);
      if (!useConstant && shouldWrapVector(i, numVectors, dictPct)) {
        for (auto child = 0; child < vector->childrenSize(); ++child) {
          wrapDictionaryRecursive(vector->childAt(child));
        }
      }
      vectors.push_back(std::move(vector));
    }
    return vectors;
  }

  ExchangeRunStats run(
      const std::vector<RowVectorPtr>& vectors,
      int32_t numDestinations,
      int32_t taskWidth,
      ExchangeMode mode) {
    VELOX_CHECK(!vectors.empty());
    VELOX_CHECK_GT(numDestinations, 0);
    VELOX_CHECK_GT(taskWidth, 0);

    core::PlanNodePtr plan;
    core::PlanNodeId exchangeId;
    core::PlanNodeId leafPartitionedOutputId;
    core::PlanNodeId finalAggPartitionedOutputId;

    std::vector<std::shared_ptr<Task>> leafTasks;
    std::vector<std::shared_ptr<Task>> finalAggTasks;
    std::vector<exec::Split> finalAggSplits;

    RowVectorPtr expected;

    const auto startUs = getCurrentTimeMicro();
    BENCHMARK_SUSPEND {
      configureQuerySettings(mode);
      const auto iteration = ++iteration_;

      // leafPlan: PartitionedOutput/kPartitioned(1) <-- Values(0)
      std::vector<std::string> leafTaskIds;
      auto leafPlanBuilder = exec::test::PlanBuilder().values(vectors, true);
      if (numDestinations == 1) {
        leafPlanBuilder.partitionedOutput({}, 1);
      } else {
        leafPlanBuilder.partitionedOutput({"c0"}, numDestinations);
      }
      auto leafPlan =
          leafPlanBuilder.capturePlanNodeId(leafPartitionedOutputId).planNode();

      for (int32_t counter = 0; counter < numDestinations; ++counter) {
        auto leafTaskId = makeTaskId(iteration, "leaf", counter);
        leafTaskIds.push_back(leafTaskId);
        auto leafTask = makeTask(leafTaskId, leafPlan, counter);
        leafTasks.push_back(leafTask);
        leafTask->start(taskWidth);
      }

      // finalAggPlan: PartitionedOutput/kPartitioned(2) <-- Agg/kSingle(1) <--
      // Exchange(0)
      core::PlanNodePtr finalAggPlan =
          exec::test::PlanBuilder()
              .exchange(leafPlan->outputType(), "Presto")
              .capturePlanNodeId(exchangeId)
              .singleAggregation({}, {"count(1)"})
              .partitionedOutput({}, 1)
              .capturePlanNodeId(finalAggPartitionedOutputId)
              .planNode();

      for (int i = 0; i < numDestinations; i++) {
        auto taskId = makeTaskId(iteration, "final-agg", i);
        finalAggSplits.push_back(
            exec::Split(std::make_shared<exec::RemoteConnectorSplit>(taskId)));
        auto finalAggTask = makeTask(taskId, finalAggPlan, i);
        finalAggTasks.push_back(finalAggTask);
        finalAggTask->start(taskWidth);
        addRemoteSplits(finalAggTask, leafTaskIds);
      }

      expected = makeRowVector({makeFlatVector<int64_t>(1, [&](auto /*row*/) {
        return vectors.size() * vectors[0]->size() * numDestinations *
            taskWidth;
      })});

      // plan: Agg/kSingle(1) <-- Exchange (0)
      plan = exec::test::PlanBuilder()
                 .exchange(finalAggPlan->outputType(), "Presto")
                 .singleAggregation({}, {"sum(a0)"})
                 .planNode();
    };

    exec::test::AssertQueryBuilder(plan)
        .splits(finalAggSplits)
        .assertResults(expected);

    ExchangeRunStats stats;
    BENCHMARK_SUSPEND {
      stats.wallUs = getCurrentTimeMicro() - startUs;

      for (const auto& task : leafTasks) {
        const auto& taskStats = task->taskStats();
        const auto& planStats = toPlanStats(taskStats);
        auto& taskPartitionedOutputStats =
            planStats.at(leafPartitionedOutputId);
        stats.partitionedOutputStats += taskPartitionedOutputStats;
      }

      for (const auto& task : finalAggTasks) {
        const auto& taskStats = task->taskStats();
        const auto& planStats = toPlanStats(taskStats);

        auto& taskPartitionedOutputStats =
            planStats.at(finalAggPartitionedOutputId);
        stats.partitionedOutputStats += taskPartitionedOutputStats;

        auto& taskExchangeStats = planStats.at(exchangeId);
        stats.exchangeStats += taskExchangeStats;
      }
    };

    return stats;
  }

 private:
  static constexpr int64_t kMaxMemory = 6UL << 30; // 6GB

  void configureQuerySettings(ExchangeMode mode) {
    configSettings_[core::QueryConfig::kMaxPartitionedOutputBufferSize] =
        fmt::format("{}", FLAGS_exchange_buffer_mb << 20);
    configSettings_[core::QueryConfig::kOptimizedPartitionedOutputEnabled] =
        mode == ExchangeMode::kOptimized ? "true" : "false";
    configSettings_[core::QueryConfig::kOptimizedHashPartitionFunctionEnabled] =
        mode == ExchangeMode::kOptimized ? "true" : "false";
  }

  static std::string
  makeTaskId(int32_t iteration, const std::string& prefix, int num) {
    return fmt::format("local://{}-{}-{}", iteration, prefix, num);
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      std::shared_ptr<const core::PlanNode> planNode,
      int destination,
      Consumer consumer = nullptr,
      int64_t maxMemory = kMaxMemory) {
    auto configCopy = configSettings_;
    auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(std::move(configCopy)));
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), maxMemory));
    core::PlanFragment planFragment{planNode};
    return Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel,
        std::move(consumer));
  }

  void addRemoteSplits(
      std::shared_ptr<Task> task,
      const std::vector<std::string>& remoteTaskIds) {
    for (const auto& taskId : remoteTaskIds) {
      auto split =
          exec::Split(std::make_shared<RemoteConnectorSplit>(taskId), -1);
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
  }

  std::unordered_map<std::string, std::string> configSettings_;
  // Serial number to differentiate consecutive benchmark repeats.
  static int32_t iteration_;
};

int32_t ExchangeBenchmark::iteration_;

std::unique_ptr<ExchangeBenchmark> bm;

void benchmarkExchange(
    unsigned int iters,
    const ExchangeInputSpec& input,
    int32_t numDestinations,
    ExchangeMode mode,
    int32_t dictPct,
    int32_t constantPct,
    int32_t nullPct) {
  auto vectors = bm->makeRows(
      input.type,
      input.numVectors,
      input.rowsPerVector,
      dictPct,
      constantPct,
      nullPct);
  auto stats = runBenchmarkIterations(iters, [&]() {
    return bm->run(vectors, numDestinations, FLAGS_task_width, mode);
  });
  benchmarkResults.push_back(
      {fmt::format(
           "{}_p{}_d{}_c{}_n{}",
           input.name,
           numDestinations,
           dictPct,
           constantPct,
           nullPct),
       mode,
       std::move(stats)});
}

#define EXCHANGE_BENCHMARK_NAMED_PARAM(name, param_name, ...) \
  BENCHMARK_IMPL(                                             \
      FB_CONCATENATE(name, FB_CONCATENATE(_, param_name)),    \
      FOLLY_PP_STRINGIZE(param_name),                         \
      iters,                                                  \
      unsigned,                                               \
      iters) {                                                \
    name(iters, ##__VA_ARGS__);                               \
  }

// Same as EXCHANGE_BENCHMARK_NAMED_PARAM but the "%" prefix on the
// stringized name marks this benchmark as relative — folly reports its
// time/iter as a percentage of the preceding non-relative baseline.
#define EXCHANGE_BENCHMARK_NAMED_PARAM_RELATIVE(name, param_name, ...) \
  BENCHMARK_IMPL(                                                      \
      FB_CONCATENATE(name, FB_CONCATENATE(_, param_name)),             \
      "%" FOLLY_PP_STRINGIZE(param_name),                              \
      iters,                                                           \
      unsigned,                                                        \
      iters) {                                                         \
    name(iters, ##__VA_ARGS__);                                        \
  }

// ── Benchmarks: input spec × p (numDestinations) × dictPct × constantPct ×
// nullPct × mode ─

#define EXCHANGE_BENCHMARK_INPUT_IMPL(                                                                   \
    _register,                                                                                           \
    _case_name,                                                                                          \
    _input_expr,                                                                                         \
    _num_destinations,                                                                                   \
    _mode_name,                                                                                          \
    _dict_pct,                                                                                           \
    _constant_pct,                                                                                       \
    _null_pct,                                                                                           \
    _mode)                                                                                               \
  _register(                                                                                             \
      benchmarkExchange,                                                                                 \
      _case_name##_p##_num_destinations##_d##_dict_pct##_c##_constant_pct##_n##_null_pct##_##_mode_name, \
      _input_expr,                                                                                       \
      _num_destinations,                                                                                 \
      ExchangeMode::_mode,                                                                               \
      _dict_pct,                                                                                         \
      _constant_pct,                                                                                     \
      _null_pct)

#define EXCHANGE_BENCHMARK_INPUT(     \
    _case_name,                       \
    _input_expr,                      \
    _num_destinations,                \
    _mode_name,                       \
    _dict_pct,                        \
    _constant_pct,                    \
    _null_pct,                        \
    _mode)                            \
  EXCHANGE_BENCHMARK_INPUT_IMPL(      \
      EXCHANGE_BENCHMARK_NAMED_PARAM, \
      _case_name,                     \
      _input_expr,                    \
      _num_destinations,              \
      _mode_name,                     \
      _dict_pct,                      \
      _constant_pct,                  \
      _null_pct,                      \
      _mode)

#define EXCHANGE_BENCHMARK_RELATIVE_INPUT(     \
    _case_name,                                \
    _input_expr,                               \
    _num_destinations,                         \
    _mode_name,                                \
    _dict_pct,                                 \
    _constant_pct,                             \
    _null_pct,                                 \
    _mode)                                     \
  EXCHANGE_BENCHMARK_INPUT_IMPL(               \
      EXCHANGE_BENCHMARK_NAMED_PARAM_RELATIVE, \
      _case_name,                              \
      _input_expr,                             \
      _num_destinations,                       \
      _mode_name,                              \
      _dict_pct,                               \
      _constant_pct,                           \
      _null_pct,                               \
      _mode)

// `normal` is the baseline (regular BENCHMARK); `optimized` is reported
// relative to it via the "%" prefix folly recognizes, so the "relative"
// column in the output shows optimized's speedup over normal.
#define EXCHANGE_BENCHMARK_MODES(    \
    _case_name,                      \
    _input_expr,                     \
    _num_destinations,               \
    _dict_pct,                       \
    _constant_pct,                   \
    _null_pct)                       \
  EXCHANGE_BENCHMARK_INPUT(          \
      _case_name,                    \
      _input_expr,                   \
      _num_destinations,             \
      normal,                        \
      _dict_pct,                     \
      _constant_pct,                 \
      _null_pct,                     \
      kNormal);                      \
  EXCHANGE_BENCHMARK_RELATIVE_INPUT( \
      _case_name,                    \
      _input_expr,                   \
      _num_destinations,             \
      optimized,                     \
      _dict_pct,                     \
      _constant_pct,                 \
      _null_pct,                     \
      kOptimized)

#define EXCHANGE_BENCHMARK_DESTINATIONS(                                  \
    _case_name, _input_expr, _dict_pct, _constant_pct, _null_pct)         \
  EXCHANGE_BENCHMARK_MODES(                                               \
      _case_name, _input_expr, 1, _dict_pct, _constant_pct, _null_pct);   \
  EXCHANGE_BENCHMARK_MODES(                                               \
      _case_name, _input_expr, 4, _dict_pct, _constant_pct, _null_pct);   \
  EXCHANGE_BENCHMARK_MODES(                                               \
      _case_name, _input_expr, 16, _dict_pct, _constant_pct, _null_pct);  \
  EXCHANGE_BENCHMARK_MODES(                                               \
      _case_name, _input_expr, 100, _dict_pct, _constant_pct, _null_pct); \
  EXCHANGE_BENCHMARK_MODES(                                               \
      _case_name, _input_expr, 1000, _dict_pct, _constant_pct, _null_pct)

#define EXCHANGE_BENCHMARK_CASE(_case_name, _input_expr)               \
  EXCHANGE_BENCHMARK_DESTINATIONS(_case_name, _input_expr, 0, 0, 0);   \
  EXCHANGE_BENCHMARK_DESTINATIONS(_case_name, _input_expr, 0, 0, 50);  \
  EXCHANGE_BENCHMARK_DESTINATIONS(_case_name, _input_expr, 0, 0, 100); \
  BENCHMARK_DRAW_LINE();

#define EXCHANGE_BENCHMARK_CONSTANT_CASE(_case_name, _input_expr)        \
  EXCHANGE_BENCHMARK_DESTINATIONS(_case_name, _input_expr, 0, 100, 0);   \
  EXCHANGE_BENCHMARK_DESTINATIONS(_case_name, _input_expr, 0, 100, 100); \
  BENCHMARK_DRAW_LINE();

#define EXCHANGE_BENCHMARK_DICTIONARY_CASE(_case_name, _input_expr)      \
  EXCHANGE_BENCHMARK_DESTINATIONS(_case_name, _input_expr, 100, 0, 0);   \
  EXCHANGE_BENCHMARK_DESTINATIONS(_case_name, _input_expr, 100, 0, 100); \
  BENCHMARK_DRAW_LINE();

EXCHANGE_BENCHMARK_CASE(
    10K_Boolean_col1,
    makeInputSpec(SimpleColType::kBoolean, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_Boolean_col4,
    makeInputSpec(SimpleColType::kBoolean, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_Boolean_col16,
    makeInputSpec(SimpleColType::kBoolean, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Boolean_col1,
    makeInputSpec(SimpleColType::kBoolean, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Boolean_col4,
    makeInputSpec(SimpleColType::kBoolean, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Boolean_col16,
    makeInputSpec(SimpleColType::kBoolean, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Boolean_col1,
    makeInputSpec(SimpleColType::kBoolean, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Boolean_col4,
    makeInputSpec(SimpleColType::kBoolean, 4));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Boolean_col16,
    makeInputSpec(SimpleColType::kBoolean, 16));
EXCHANGE_BENCHMARK_CASE(
    10K_Tinyint_col1,
    makeInputSpec(SimpleColType::kTinyint, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_Tinyint_col4,
    makeInputSpec(SimpleColType::kTinyint, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_Tinyint_col16,
    makeInputSpec(SimpleColType::kTinyint, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Tinyint_col1,
    makeInputSpec(SimpleColType::kTinyint, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Tinyint_col4,
    makeInputSpec(SimpleColType::kTinyint, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Tinyint_col16,
    makeInputSpec(SimpleColType::kTinyint, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Tinyint_col1,
    makeInputSpec(SimpleColType::kTinyint, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Tinyint_col4,
    makeInputSpec(SimpleColType::kTinyint, 4));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Tinyint_col16,
    makeInputSpec(SimpleColType::kTinyint, 16));
EXCHANGE_BENCHMARK_CASE(
    10K_Integer_col1,
    makeInputSpec(SimpleColType::kInteger, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_Integer_col4,
    makeInputSpec(SimpleColType::kInteger, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_Integer_col16,
    makeInputSpec(SimpleColType::kInteger, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Integer_col1,
    makeInputSpec(SimpleColType::kInteger, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Integer_col4,
    makeInputSpec(SimpleColType::kInteger, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Integer_col16,
    makeInputSpec(SimpleColType::kInteger, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Integer_col1,
    makeInputSpec(SimpleColType::kInteger, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Integer_col4,
    makeInputSpec(SimpleColType::kInteger, 4));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Integer_col16,
    makeInputSpec(SimpleColType::kInteger, 16));
EXCHANGE_BENCHMARK_CASE(
    10K_Bigint_col1,
    makeInputSpec(SimpleColType::kBigint, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_Bigint_col4,
    makeInputSpec(SimpleColType::kBigint, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_Bigint_col16,
    makeInputSpec(SimpleColType::kBigint, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Bigint_col1,
    makeInputSpec(SimpleColType::kBigint, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Bigint_col4,
    makeInputSpec(SimpleColType::kBigint, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Bigint_col16,
    makeInputSpec(SimpleColType::kBigint, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Bigint_col1,
    makeInputSpec(SimpleColType::kBigint, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Bigint_col4,
    makeInputSpec(SimpleColType::kBigint, 4));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Bigint_col16,
    makeInputSpec(SimpleColType::kBigint, 16));
EXCHANGE_BENCHMARK_CASE(
    10K_Hugeint_col1,
    makeInputSpec(SimpleColType::kHugeint, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_Hugeint_col4,
    makeInputSpec(SimpleColType::kHugeint, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_Hugeint_col16,
    makeInputSpec(SimpleColType::kHugeint, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Hugeint_col1,
    makeInputSpec(SimpleColType::kHugeint, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Hugeint_col4,
    makeInputSpec(SimpleColType::kHugeint, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Hugeint_col16,
    makeInputSpec(SimpleColType::kHugeint, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Hugeint_col1,
    makeInputSpec(SimpleColType::kHugeint, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Hugeint_col4,
    makeInputSpec(SimpleColType::kHugeint, 4));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Hugeint_col16,
    makeInputSpec(SimpleColType::kHugeint, 16));
EXCHANGE_BENCHMARK_CASE(
    10K_LongDecimal_col1,
    makeInputSpec(SimpleColType::kLongDecimal, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_LongDecimal_col4,
    makeInputSpec(SimpleColType::kLongDecimal, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_LongDecimal_col16,
    makeInputSpec(SimpleColType::kLongDecimal, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_LongDecimal_col1,
    makeInputSpec(SimpleColType::kLongDecimal, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_LongDecimal_col4,
    makeInputSpec(SimpleColType::kLongDecimal, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_LongDecimal_col16,
    makeInputSpec(SimpleColType::kLongDecimal, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_LongDecimal_col1,
    makeInputSpec(SimpleColType::kLongDecimal, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_LongDecimal_col4,
    makeInputSpec(SimpleColType::kLongDecimal, 4));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_LongDecimal_col16,
    makeInputSpec(SimpleColType::kLongDecimal, 16));
EXCHANGE_BENCHMARK_CASE(
    10K_Double_col1,
    makeInputSpec(SimpleColType::kDouble, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_Double_col4,
    makeInputSpec(SimpleColType::kDouble, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_Double_col16,
    makeInputSpec(SimpleColType::kDouble, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Double_col1,
    makeInputSpec(SimpleColType::kDouble, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Double_col4,
    makeInputSpec(SimpleColType::kDouble, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Double_col16,
    makeInputSpec(SimpleColType::kDouble, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Double_col1,
    makeInputSpec(SimpleColType::kDouble, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Double_col4,
    makeInputSpec(SimpleColType::kDouble, 4));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Double_col16,
    makeInputSpec(SimpleColType::kDouble, 16));
EXCHANGE_BENCHMARK_CASE(
    10K_Varchar_col1,
    makeInputSpec(SimpleColType::kVarchar, 1));
EXCHANGE_BENCHMARK_CASE(
    10K_Varchar_col4,
    makeInputSpec(SimpleColType::kVarchar, 4));
EXCHANGE_BENCHMARK_CASE(
    10K_Varchar_col16,
    makeInputSpec(SimpleColType::kVarchar, 16));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Varchar_col1,
    makeInputSpec(SimpleColType::kVarchar, 1));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Varchar_col4,
    makeInputSpec(SimpleColType::kVarchar, 4));
EXCHANGE_BENCHMARK_CONSTANT_CASE(
    10K_Varchar_col16,
    makeInputSpec(SimpleColType::kVarchar, 16));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Varchar_col1,
    makeInputSpec(SimpleColType::kVarchar, 1));
EXCHANGE_BENCHMARK_DICTIONARY_CASE(
    10K_Varchar_col4,
    makeInputSpec(SimpleColType::kVarchar, 4));

// Temporarily disable this benchmark because of large memory usage.
// EXCHANGE_BENCHMARK_DICTIONARY_CASE(
//     10K_Varchar_col16,
//     makeInputSpec(SimpleColType::kVarchar, 16));

// The ARRAY/MAP complex type benchmarks are temporarily disabled because
// makeColumn does not generate ARRAY/MAP yet.
// EXCHANGE_BENCHMARK_CASE(Deep10K, makeInputSpec(ExchangeInputKind::kDeep10K));
// EXCHANGE_BENCHMARK_CASE(Deep50, makeInputSpec(ExchangeInputKind::kDeep50));
// EXCHANGE_BENCHMARK_CASE(Struct1K,
// makeInputSpec(ExchangeInputKind::kStruct1K));

// Nested ROW columns (makeColumn supports ROW recursively).
EXCHANGE_BENCHMARK_CASE(
    Nested1_1K,
    makeInputSpec(ExchangeInputKind::kNested1_1K));
EXCHANGE_BENCHMARK_CASE(
    Nested2_1K,
    makeInputSpec(ExchangeInputKind::kNested2_1K));
EXCHANGE_BENCHMARK_CASE(
    Nested3_1K,
    makeInputSpec(ExchangeInputKind::kNested3_1K));

#undef EXCHANGE_BENCHMARK_CASE
#undef EXCHANGE_BENCHMARK_DICTIONARY_CASE
#undef EXCHANGE_BENCHMARK_CONSTANT_CASE
#undef EXCHANGE_BENCHMARK_DESTINATIONS
#undef EXCHANGE_BENCHMARK_MODES
#undef EXCHANGE_BENCHMARK_RELATIVE_INPUT
#undef EXCHANGE_BENCHMARK_INPUT
#undef EXCHANGE_BENCHMARK_INPUT_IMPL
#undef EXCHANGE_BENCHMARK_NAMED_PARAM_RELATIVE
#undef EXCHANGE_BENCHMARK_NAMED_PARAM

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  if (!isRegisteredNamedVectorSerde("Presto")) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  exec::ExchangeSource::registerFactory(exec::test::createLocalExchangeSource);

  bm = std::make_unique<ExchangeBenchmark>();
  folly::runBenchmarks();
  std::fflush(stdout);
  std::fflush(stderr);
  std::cout << std::endl << "Exchange detailed stats:" << std::endl;
  printAllExchangeStats();
  bm.reset();

  return 0;
}
