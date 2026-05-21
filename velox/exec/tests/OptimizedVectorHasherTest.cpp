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
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/OptimizedVectorHasher.h"
#include "velox/exec/VectorHasher.h"
#include "velox/type/tests/utils/CustomTypesForTesting.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

class OptimizedVectorHasherTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  BufferPtr makeIndices(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t)> indexAt) {
    auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (vector_size_t i = 0; i < size; ++i) {
      rawIndices[i] = indexAt(i);
    }
    return indices;
  }

  static SelectivityVector makeOddRows(vector_size_t size) {
    SelectivityVector oddRows(size);
    for (vector_size_t i = 0; i < size; i += 2) {
      oddRows.setValid(i, false);
    }
    oddRows.updateBounds();
    return oddRows;
  }

  void compareHashes(
      const TypePtr& type,
      const VectorPtr& vector,
      const SelectivityVector& rows,
      bool mix,
      uint64_t seed = 0) {
    auto expectedHasher = VectorHasher::create(type, 0);
    auto actualHasher = OptimizedVectorHasher::create(type, 0);

    raw_vector<uint64_t> expected(vector->size(), pool());
    raw_vector<uint64_t> actual(vector->size(), pool());
    if (mix) {
      std::iota(expected.begin(), expected.end(), seed);
      std::iota(actual.begin(), actual.end(), seed);
    } else {
      std::fill(expected.begin(), expected.end(), 0);
      std::fill(actual.begin(), actual.end(), 0);
    }

    expectedHasher->decode(*vector, rows);
    actualHasher->decode(*vector, rows);

    expectedHasher->hash(rows, mix, expected);
    actualHasher->hash(rows, mix, actual);

    for (vector_size_t i = 0; i < vector->size(); ++i) {
      EXPECT_EQ(expected[i], actual[i]) << "at " << i;
    }
  }

  void comparePrecomputed(
      const TypePtr& type,
      const VectorPtr& value,
      vector_size_t size,
      bool mix,
      uint64_t seed = 0) {
    auto expectedHasher = VectorHasher::create(type, 0);
    auto actualHasher = OptimizedVectorHasher::create(type, 0);

    raw_vector<uint64_t> expected(size, pool());
    raw_vector<uint64_t> actual(size, pool());
    if (mix) {
      std::iota(expected.begin(), expected.end(), seed);
      std::iota(actual.begin(), actual.end(), seed);
    } else {
      std::fill(expected.begin(), expected.end(), 0);
      std::fill(actual.begin(), actual.end(), 0);
    }

    const SelectivityVector rows(size);
    expectedHasher->precompute(*value);
    actualHasher->precompute(*value);

    expectedHasher->hashPrecomputed(rows, mix, expected);
    actualHasher->hashPrecomputed(mix, actual);

    for (vector_size_t i = 0; i < size; ++i) {
      EXPECT_EQ(expected[i], actual[i]) << "at " << i;
    }
  }
};

TEST_F(OptimizedVectorHasherTest, flat) {
  auto vector = BaseVector::create(BIGINT(), 100, pool());
  auto flatVector = vector->asFlatVector<int64_t>();
  for (vector_size_t i = 0; i < 100; ++i) {
    if (i % 5 == 0) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, i);
    }
  }

  const SelectivityVector allRows(100);
  const auto oddRows = makeOddRows(100);

  compareHashes(BIGINT(), vector, oddRows, false);
  compareHashes(BIGINT(), vector, allRows, false);
  compareHashes(BIGINT(), vector, allRows, true, 10);

  flatVector->setNull(0, true);
  comparePrecomputed(BIGINT(), vector, 100, false);

  flatVector->setNull(0, false);
  flatVector->set(0, 7);
  comparePrecomputed(BIGINT(), vector, 100, false);

  flatVector->set(0, 55);
  comparePrecomputed(BIGINT(), vector, 100, true, 20);
}

TEST_F(OptimizedVectorHasherTest, boolFlat) {
  constexpr vector_size_t kSize = 137;
  auto vector = makeFlatVector<bool>(
      kSize,
      [](vector_size_t row) { return row % 7 == 0 || row % 11 == 3; },
      [](vector_size_t row) { return row % 13 == 5; });
  const SelectivityVector allRows(vector->size());
  const auto oddRows = makeOddRows(vector->size());

  compareHashes(BOOLEAN(), vector, oddRows, false);
  compareHashes(BOOLEAN(), vector, allRows, false);
  compareHashes(BOOLEAN(), vector, allRows, true, 17);

  vector = makeFlatVector<bool>(
      kSize, [](vector_size_t row) { return row % 5 < 2; });
  compareHashes(BOOLEAN(), vector, allRows, false);
  compareHashes(BOOLEAN(), vector, allRows, true, 23);
}

TEST_F(OptimizedVectorHasherTest, nans) {
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  auto vector = makeFlatVector<double>({1.0, -1.0, kNaN, kSNaN, 0.0, -0.0});
  const SelectivityVector allRows(vector->size());

  compareHashes(DOUBLE(), vector, allRows, false);
  compareHashes(DOUBLE(), vector, allRows, true, 15);
}

TEST_F(OptimizedVectorHasherTest, nonNullConstant) {
  auto vector = BaseVector::createConstant(INTEGER(), 123, 6, pool());
  const SelectivityVector allRows(vector->size());
  const auto oddRows = makeOddRows(vector->size());

  compareHashes(INTEGER(), vector, oddRows, false);
  compareHashes(INTEGER(), vector, allRows, false);
  compareHashes(INTEGER(), vector, allRows, true, 7);
}

TEST_F(OptimizedVectorHasherTest, nullConstant) {
  auto vector = BaseVector::createNullConstant(INTEGER(), 6, pool());
  const SelectivityVector allRows(vector->size());
  const auto oddRows = makeOddRows(vector->size());

  compareHashes(INTEGER(), vector, oddRows, false);
  compareHashes(INTEGER(), vector, allRows, false);
  compareHashes(INTEGER(), vector, allRows, true, 11);
}

TEST_F(OptimizedVectorHasherTest, scalarHashPrecomputed) {
  auto vector = makeFlatVector<int64_t>({123});
  auto hasher = OptimizedVectorHasher::create(BIGINT(), 0);
  hasher->precompute(*vector);

  raw_vector<uint64_t> expected(1, pool());
  expected[0] = 0;
  hasher->hashPrecomputed(false, expected);
  EXPECT_EQ(hasher->hashPrecomputed(false, 19), expected[0]);

  expected[0] = 19;
  hasher->hashPrecomputed(true, expected);
  EXPECT_EQ(hasher->hashPrecomputed(true, 19), expected[0]);
}

TEST_F(OptimizedVectorHasherTest, scalarHashConstant) {
  auto vector = BaseVector::createConstant(INTEGER(), 123, 6, pool());
  const SelectivityVector allRows(vector->size());
  auto hasher = OptimizedVectorHasher::create(INTEGER(), 0);
  hasher->decode(*vector, allRows);

  raw_vector<uint64_t> expected(vector->size(), pool());
  std::fill(expected.begin(), expected.end(), 0);
  hasher->hash(false, expected);
  auto actual = hasher->hashConstant(false, 19);
  ASSERT_TRUE(actual.has_value());
  EXPECT_EQ(actual.value(), expected[0]);

  std::fill(expected.begin(), expected.end(), 19);
  hasher->hash(true, expected);
  actual = hasher->hashConstant(true, 19);
  ASSERT_TRUE(actual.has_value());
  EXPECT_EQ(actual.value(), expected[0]);
}

TEST_F(OptimizedVectorHasherTest, scalarHashConstantEmpty) {
  auto vector = BaseVector::createConstant(INTEGER(), 123, 0, pool());
  const SelectivityVector rows(vector->size());
  auto hasher = OptimizedVectorHasher::create(INTEGER(), 0);
  hasher->decode(*vector, rows);

  EXPECT_EQ(hasher->hashConstant(false, 19), std::nullopt);
  EXPECT_EQ(hasher->hashConstant(true, 19), std::nullopt);
}

TEST_F(OptimizedVectorHasherTest, unknown) {
  auto vector = makeAllNullFlatVector<UnknownValue>(100);
  const SelectivityVector allRows(vector->size());
  const auto oddRows = makeOddRows(vector->size());

  compareHashes(UNKNOWN(), vector, oddRows, false);
  compareHashes(UNKNOWN(), vector, allRows, false);
  compareHashes(UNKNOWN(), vector, allRows, true, 0);
}

TEST_F(OptimizedVectorHasherTest, dictionary) {
  auto base = makeNullableFlatVector<int64_t>({10, 20, std::nullopt, 40, 50});
  constexpr vector_size_t kSize = 100;
  auto dictionary = BaseVector::wrapInDictionary(
      makeNulls(kSize, [&](vector_size_t row) { return row == 1 || row == 7; }),
      makeIndices(kSize, [&](vector_size_t row) { return row % base->size(); }),
      kSize,
      base);
  const SelectivityVector allRows(dictionary->size());
  const auto oddRows = makeOddRows(dictionary->size());

  compareHashes(BIGINT(), dictionary, oddRows, false);
  compareHashes(BIGINT(), dictionary, allRows, false);
  compareHashes(BIGINT(), dictionary, allRows, true, 10);
}

TEST_F(OptimizedVectorHasherTest, customComparison) {
  auto vector = makeNullableFlatVector<int64_t>(
      {0, 1, 256, 257, std::nullopt, 512, 513},
      BIGINT_TYPE_WITH_CUSTOM_COMPARISON());
  const SelectivityVector allRows(vector->size());

  compareHashes(BIGINT_TYPE_WITH_CUSTOM_COMPARISON(), vector, allRows, false);
  compareHashes(BIGINT_TYPE_WITH_CUSTOM_COMPARISON(), vector, allRows, true, 9);
}

TEST_F(OptimizedVectorHasherTest, customComparisonArray) {
  auto vector = makeNullableArrayVector<int64_t>(
      {{0, 1, 2},
       {256, 257, 258},
       {512, 513, 514},
       {3, 4, 5},
       {259, 260, 261},
       {515, 516, 517},
       {std::nullopt}},
      ARRAY(BIGINT_TYPE_WITH_CUSTOM_COMPARISON()));
  const SelectivityVector allRows(vector->size());

  compareHashes(
      ARRAY(BIGINT_TYPE_WITH_CUSTOM_COMPARISON()), vector, allRows, false);
}

TEST_F(OptimizedVectorHasherTest, customComparisonMap) {
  auto vector = makeNullableMapVector<int64_t, int64_t>(
      {std::vector<std::pair<int64_t, std::optional<int64_t>>>{
           {0, 10}, {1, 11}, {2, 12}},
       std::vector<std::pair<int64_t, std::optional<int64_t>>>{
           {256, 266}, {257, 267}, {258, 268}},
       std::vector<std::pair<int64_t, std::optional<int64_t>>>{
           {512, 522}, {513, 523}, {514, 524}},
       std::vector<std::pair<int64_t, std::optional<int64_t>>>{
           {3, 103}, {4, 104}, {5, 105}},
       std::vector<std::pair<int64_t, std::optional<int64_t>>>{
           {259, 359}, {260, 360}, {261, 361}},
       std::vector<std::pair<int64_t, std::optional<int64_t>>>{
           {515, 615}, {516, 616}, {517, 617}},
       std::vector<std::pair<int64_t, std::optional<int64_t>>>{
           {0, std::nullopt}}},
      MAP(BIGINT_TYPE_WITH_CUSTOM_COMPARISON(),
          BIGINT_TYPE_WITH_CUSTOM_COMPARISON()));
  const SelectivityVector allRows(vector->size());

  compareHashes(
      MAP(BIGINT_TYPE_WITH_CUSTOM_COMPARISON(),
          BIGINT_TYPE_WITH_CUSTOM_COMPARISON()),
      vector,
      allRows,
      false);
}

TEST_F(OptimizedVectorHasherTest, customComparisonRow) {
  auto vector = makeRowVector(
      {"a"},
      {makeNullableFlatVector<int64_t>(
          {std::nullopt, 0, 1, 256, 257, 512, 513},
          BIGINT_TYPE_WITH_CUSTOM_COMPARISON())});
  const SelectivityVector allRows(vector->size());

  compareHashes(vector->type(), vector, allRows, false);
}

TEST_F(OptimizedVectorHasherTest, precompute) {
  auto value = makeNullableFlatVector<int64_t>({std::nullopt});
  comparePrecomputed(BIGINT(), value, 100, false);

  value = makeNullableFlatVector<int64_t>({7});
  comparePrecomputed(BIGINT(), value, 100, false);

  value = makeNullableFlatVector<int64_t>({55});
  comparePrecomputed(BIGINT(), value, 100, true, 100);
}

TEST_F(OptimizedVectorHasherTest, typeMismatch) {
  auto hasher = OptimizedVectorHasher::create(BIGINT(), 0);
  auto vector = makeFlatVector<StringView>({"a", "b", "c"});
  SelectivityVector rows(vector->size());

  VELOX_ASSERT_THROW(
      hasher->decode(*vector, rows), "Type mismatch: BIGINT vs. VARCHAR");
}

} // namespace
