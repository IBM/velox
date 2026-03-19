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
#include "velox/experimental/cudf/exec/CudfConversion.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace {

class CudfWindowTest : public testing::Test,
                       public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    functions::prestosql::registerAllScalarFunctions();
    window::prestosql::registerAllWindowFunctions();
    parse::registerTypeResolver();
    cudf_velox::CudfConfig::getInstance().allowCpuFallback = false;
    cudf_velox::registerCudf();
  }

  void TearDown() override {
    cudf_velox::unregisterCudf();
  }
};

TEST_F(CudfWindowTest, rowNumberPartitionOrder) {
  auto data = makeRowVector(
      {"id", "val"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 30, 15, 25, 35}),
      });

  auto plan = PlanBuilder()
                  .values({data})
                  .window({"row_number() over (partition by id order by val)"})
                  .orderBy({"id ASC NULLS LAST", "val ASC NULLS LAST"}, false)
                  .planNode();

  auto expected = makeRowVector(
      {"id", "val", "w0"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 30, 15, 25, 35}),
          makeFlatVector<int64_t>({1, 2, 3, 1, 2, 3}),
      });

  AssertQueryBuilder(plan).assertResults(expected);
}

TEST_F(CudfWindowTest, lagLead) {
  auto data = makeRowVector(
      {"id", "val"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 2, 2}),
          makeFlatVector<int64_t>({100, 200, 300, 10, 20}),
      });

  auto plan = PlanBuilder()
                  .values({data})
                  .window({
                      "lag(val, 1) over (partition by id order by val) as lag_val",
                      "lead(val, 1) over (partition by id order by val) as lead_val",
                  })
                  .orderBy({"id ASC NULLS LAST", "val ASC NULLS LAST"}, false)
                  .planNode();

  auto lagValues = makeNullableFlatVector<int64_t>(
      {std::nullopt, 100, 200, std::nullopt, 10});
  auto leadValues = makeNullableFlatVector<int64_t>(
      {200, 300, std::nullopt, 20, std::nullopt});
  auto expected = makeRowVector(
      {"id", "val", "lag_val", "lead_val"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 2, 2}),
          makeFlatVector<int64_t>({100, 200, 300, 10, 20}),
          lagValues,
          leadValues,
      });

  AssertQueryBuilder(plan).assertResults(expected);
}

} // namespace
