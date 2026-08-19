/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Copyright (c) 2026 IBM Corporation.
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

#include "velox/expression/PrestoCastHooksV2.h"

#include <gtest/gtest.h>

#include "velox/common/base/Status.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/EvalCtx.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
namespace {

class PrestoCastHooksV2Test : public testing::Test,
                              public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  // Builds an EvalCtx detached from any expression so a vector hook can
  // be called directly and its setStatus side effects observed.  Sets
  // throwOnError=false so errors accumulate in context.errors() instead
  // of propagating as C++ exceptions.
  std::unique_ptr<EvalCtx> makeEvalCtx() {
    auto ctx = std::make_unique<EvalCtx>(execCtx_.get());
    *ctx->mutableThrowOnError() = false;
    return ctx;
  }

  // Returns the textual user-error message recorded by 'context' at
  // 'row', or empty if no error was recorded there.
  std::string errorAt(EvalCtx& context, vector_size_t row) {
    auto errors = context.errors();
    if (errors == nullptr || row >= errors->size() ||
        !errors->hasErrorAt(row)) {
      return "";
    }
    try {
      errors->throwIfErrorAt(row);
    } catch (const VeloxUserError& ue) {
      return ue.message();
    } catch (const std::exception& e) {
      return e.what();
    }
    return "";
  }

  // Pre-allocated FlatVector ready to receive 'numRows' values of T.
  template <typename T>
  std::shared_ptr<FlatVector<T>> makeWritableFlat(
      const TypePtr& type,
      vector_size_t numRows) {
    auto base = BaseVector::create(type, numRows, pool_.get());
    return std::dynamic_pointer_cast<FlatVector<T>>(base);
  }

  core::QueryConfig defaultConfig() {
    return core::QueryConfig(std::unordered_map<std::string, std::string>{});
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

TEST_F(PrestoCastHooksV2Test, castStringToTimestampVectorSucceeds) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  auto input = makeFlatVector<StringView>(
      {"2024-01-15 12:34:56", "2024-06-01 00:00:00", "1970-01-01 00:00:00"});
  auto result = makeWritableFlat<Timestamp>(TIMESTAMP(), input->size());
  SelectivityVector rows(input->size());

  hooks.castStringToTimestampVector(rows, *input, *result, *ctx);

  EXPECT_EQ(ctx->errors(), nullptr);
  EXPECT_EQ(result->valueAt(2), Timestamp(0, 0));
}

TEST_F(PrestoCastHooksV2Test, castStringToTimestampVectorEmptyStringErrors) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  auto input = makeFlatVector<StringView>({"", "2024-01-15 00:00:00"});
  auto result = makeWritableFlat<Timestamp>(TIMESTAMP(), input->size());
  SelectivityVector rows(input->size());

  hooks.castStringToTimestampVector(rows, *input, *result, *ctx);

  // PrestoCastHooks::removeWhiteSpaces is a no-op; only the truly
  // empty input shortcuts to the "Empty string" branch.  Whitespace-
  // padded inputs reach the scalar parser, which has its own handling.
  EXPECT_EQ(errorAt(*ctx, 0), "Empty string");
  EXPECT_EQ(errorAt(*ctx, 1), "");
}

TEST_F(PrestoCastHooksV2Test, castStringToTimestampVectorInvalidErrors) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  auto input =
      makeFlatVector<StringView>({"not-a-timestamp", "2024-01-15 00:00:00"});
  auto result = makeWritableFlat<Timestamp>(TIMESTAMP(), input->size());
  SelectivityVector rows(input->size());

  hooks.castStringToTimestampVector(rows, *input, *result, *ctx);

  EXPECT_FALSE(errorAt(*ctx, 0).empty());
  EXPECT_EQ(errorAt(*ctx, 1), "");
}

TEST_F(PrestoCastHooksV2Test, castStringToDateVectorWhitespaceAndEmpty) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  auto input =
      makeFlatVector<StringView>({"  2024-03-10  ", "", "2024-06-01"});
  auto result = makeWritableFlat<int32_t>(DATE(), input->size());
  SelectivityVector rows(input->size());

  hooks.castStringToDateVector(rows, *input, *result, *ctx);

  EXPECT_EQ(errorAt(*ctx, 0), "");
  EXPECT_EQ(errorAt(*ctx, 1), "Empty string");
  EXPECT_EQ(errorAt(*ctx, 2), "");
  // 2024-06-01 is day 19'875 since 1970-01-01.
  EXPECT_EQ(result->valueAt(2), 19'875);
}

TEST_F(PrestoCastHooksV2Test, castStringToRealVectorWhitespaceAndEmpty) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  // Scalar parser strips leading whitespace itself and allows trailing
  // whitespace, so " 3.5 " parses cleanly even though Presto's
  // removeWhiteSpaces is a no-op.  The empty string short-circuits to
  // an "Empty string" error before reaching the parser.
  auto input = makeFlatVector<StringView>({"  3.5  ", "", "1.25"});
  auto result = makeWritableFlat<float>(REAL(), input->size());
  SelectivityVector rows(input->size());

  hooks.castStringToRealVector(rows, *input, *result, *ctx);

  EXPECT_EQ(result->valueAt(0), 3.5f);
  EXPECT_EQ(errorAt(*ctx, 1), "Empty string");
  EXPECT_EQ(result->valueAt(2), 1.25f);
}

TEST_F(PrestoCastHooksV2Test, castStringToRealVectorInvalidErrors) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  auto input = makeFlatVector<StringView>({"@@@", "1.5"});
  auto result = makeWritableFlat<float>(REAL(), input->size());
  SelectivityVector rows(input->size());

  hooks.castStringToRealVector(rows, *input, *result, *ctx);

  ASSERT_NE(ctx->errors(), nullptr);
  EXPECT_TRUE(ctx->errors()->hasErrorAt(0));
  EXPECT_FALSE(ctx->errors()->hasErrorAt(1));
  EXPECT_EQ(result->valueAt(1), 1.5f);
}

TEST_F(PrestoCastHooksV2Test, castStringToDoubleVectorBasic) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  auto input = makeFlatVector<StringView>({"\t2.5", "0.0", " 1e10 "});
  auto result = makeWritableFlat<double>(DOUBLE(), input->size());
  SelectivityVector rows(input->size());

  hooks.castStringToDoubleVector(rows, *input, *result, *ctx);

  EXPECT_EQ(ctx->errors(), nullptr);
  EXPECT_DOUBLE_EQ(result->valueAt(0), 2.5);
  EXPECT_DOUBLE_EQ(result->valueAt(1), 0.0);
  EXPECT_DOUBLE_EQ(result->valueAt(2), 1e10);
}

TEST_F(PrestoCastHooksV2Test, castDateToTimestampVectorNoTimeZone) {
  PrestoCastHooksV2 hooks(defaultConfig());
  // 1970-01-01 (day 0), 2024-01-15 (day 19'737).
  auto input = makeFlatVector<int32_t>({0, 19'737});
  auto result = makeWritableFlat<Timestamp>(TIMESTAMP(), input->size());
  SelectivityVector rows(input->size());

  hooks.castDateToTimestampVector(
      rows, *input, *result, /*timeZone=*/nullptr);

  EXPECT_EQ(result->valueAt(0), Timestamp(0, 0));
  EXPECT_EQ(result->valueAt(1), Timestamp(19'737LL * 86'400LL, 0));
}

TEST_F(PrestoCastHooksV2Test, castDateToTimestampVectorWithTimeZone) {
  PrestoCastHooksV2 hooks(defaultConfig());
  const auto* timeZone = tz::locateZone("America/Los_Angeles");
  ASSERT_NE(timeZone, nullptr);

  // 2024-06-01: midnight local in LA is 07:00 UTC (PDT, UTC-7).
  auto input = makeFlatVector<int32_t>({19'875});
  auto result = makeWritableFlat<Timestamp>(TIMESTAMP(), input->size());
  SelectivityVector rows(input->size());

  hooks.castDateToTimestampVector(rows, *input, *result, timeZone);

  EXPECT_EQ(result->valueAt(0), Timestamp(19'875LL * 86'400LL + 7 * 3'600, 0));
}

TEST_F(PrestoCastHooksV2Test, castDateTimestampToGMTVectorShifts) {
  PrestoCastHooksV2 hooks(defaultConfig());
  const auto* timeZone = tz::locateZone("America/Los_Angeles");
  ASSERT_NE(timeZone, nullptr);

  // Pre-populate with a local time at midnight on 2024-06-01.
  auto vec = makeFlatVector<Timestamp>(
      {Timestamp(19'875LL * 86'400LL, 0), Timestamp(0, 0)});
  SelectivityVector rows(vec->size());

  hooks.castDateTimestampToGMTVector(rows, *vec, *timeZone);

  // Both rows should shift forward by 7h.
  EXPECT_EQ(vec->valueAt(0), Timestamp(19'875LL * 86'400LL + 7 * 3'600, 0));
  EXPECT_EQ(vec->valueAt(1), Timestamp(8 * 3'600, 0));
}

TEST_F(PrestoCastHooksV2Test, castIntToTimestampVectorErrorsAllRows) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();
  SelectivityVector rows(4);

  hooks.castIntToTimestampVector(rows, *ctx);

  for (vector_size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(errorAt(*ctx, i), "Conversion to Timestamp is not supported");
  }
}

TEST_F(PrestoCastHooksV2Test, castBooleanToTimestampVectorErrorsAllRows) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();
  SelectivityVector rows(3);

  hooks.castBooleanToTimestampVector(rows, *ctx);

  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(errorAt(*ctx, i), "Conversion to Timestamp is not supported");
  }
}

TEST_F(PrestoCastHooksV2Test, castTimestampToIntVectorErrorsAllRows) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();
  SelectivityVector rows(2);

  hooks.castTimestampToIntVector(rows, *ctx);

  for (vector_size_t i = 0; i < 2; ++i) {
    EXPECT_EQ(
        errorAt(*ctx, i),
        "Conversion from Timestamp to Int is not supported");
  }
}

TEST_F(PrestoCastHooksV2Test, castDoubleToTimestampVectorErrorsAllRows) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();
  SelectivityVector rows(5);

  hooks.castDoubleToTimestampVector(rows, *ctx);

  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(errorAt(*ctx, i), "Conversion to Timestamp is not supported");
  }
}

TEST_F(PrestoCastHooksV2Test, vectorHookSkipsDeselectedRows) {
  PrestoCastHooksV2 hooks(defaultConfig());
  auto ctx = makeEvalCtx();

  auto input =
      makeFlatVector<StringView>({"bad", "2024-01-15", "also-bad", "2024-06-01"});
  auto result = makeWritableFlat<int32_t>(DATE(), input->size());
  SelectivityVector rows(input->size());
  rows.setValid(0, false);
  rows.setValid(2, false);
  rows.updateBounds();

  hooks.castStringToDateVector(rows, *input, *result, *ctx);

  // Rows 0 and 2 were deselected, so they never got an error recorded.
  EXPECT_EQ(errorAt(*ctx, 0), "");
  EXPECT_EQ(errorAt(*ctx, 1), "");
  EXPECT_EQ(errorAt(*ctx, 2), "");
  EXPECT_EQ(errorAt(*ctx, 3), "");
  EXPECT_EQ(result->valueAt(1), 19'737);
  EXPECT_EQ(result->valueAt(3), 19'875);
}

} // namespace
} // namespace facebook::velox::exec::test
