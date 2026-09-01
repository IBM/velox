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
#include <algorithm>
#include <random>

#include <gtest/gtest.h>

#include "vector/tests/utils/VectorTestBase.h"
#include "velox/vector/PartitionedVector.h"
#include "velox/vector/tests/utils/PartitionedVectorTestBase.h"

namespace facebook::velox::test {

class PartitioningVectorTest : public testing::TestWithParam<int>,
                               public test::PartitionedVectorTestBase {
 protected:
  std::mt19937 gen_ = std::mt19937(std::random_device{}());

  PartitionBuildContext ctx_;
  BufferPtr partitionOffsets_;

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void testPartitionedVector(
      VectorPtr vector,
      const std::vector<uint32_t>& partitions,
      uint32_t numPartitions) {
    // Back up the vector before calling PartitionedVector::create()
    VectorPtr vectorCopy = BaseVector::copy(*vector);
    // Build the expected vector using the reference implementation
    std::vector<VectorPtr> expectedVectors =
        partitionVectorByWrapping(vectorCopy, partitions, numPartitions);

    // Initialize buffers needed for PartitionedVector::create()
    ensureCapacity<vector_size_t>(
        ctx_.cursorPartitionOffsets, numPartitions, pool_.get());

    // Calculate the number of values for each partition
    std::vector<vector_size_t> partitionRowCounts(numPartitions, 0);
    for (auto partition : partitions) {
      partitionRowCounts[partition]++;
    }

    // Create the partitioned vector using the actual implementation
    auto partitionedVector = PartitionedVector::create(
        vector,
        partitions,
        numPartitions,
        //        partitionOffsets_,
        ctx_,
        pool_.get());
    VELOX_CHECK_NOT_NULL(partitionedVector);

    // Extract each partition and compare with expected results
    std::vector<VectorPtr> partitionedVectors;
    for (uint32_t i = 0; i < numPartitions; ++i) {
      auto partition = partitionedVector->partitionAt(i);
      partitionedVectors.push_back(partition);
    }

    for (uint32_t i = 0; i < numPartitions; ++i) {
      test::assertEqualVectors(
          expectedVectors[i], canonicalize(partitionedVectors[i]));
    }
  }

  void testVectorPartitioning(VectorPtr vector) {
    auto numRows = vector->size();
    std::vector<uint32_t> partitions(numRows);

    // Test with single partition
    std::fill(partitions.begin(), partitions.end(), 0);
    auto vectorCopy = BaseVector::copy(*vector, pool_.get());
    testPartitionedVector(vectorCopy, partitions, 1);

    // Test with two partitions
    if (vector->size() >= 3) {
      for (uint32_t i = 0; i < partitions.size(); ++i) {
        partitions[i] = i % 2;
      }
      vectorCopy = BaseVector::copy(*vector, pool_.get());
      testPartitionedVector(vectorCopy, partitions, 2);
    }

    // Test with three partitions
    for (uint32_t i = 0; i < partitions.size(); ++i) {
      partitions[i] = i % 3;
    }
    vectorCopy = BaseVector::copy(*vector, pool_.get());
    testPartitionedVector(vectorCopy, partitions, 3);

    if (vector->size() > 4) {
      // Test with four partitions where the first partition is empty
      for (uint32_t i = 0; i < partitions.size(); ++i) {
        partitions[i] = i % 3 + 1;
      }
      vectorCopy = BaseVector::copy(*vector, pool_.get());
      testPartitionedVector(vectorCopy, partitions, 4);

      // Test with four partitions where the last partition is empty
      for (uint32_t i = 0; i < partitions.size(); ++i) {
        partitions[i] = i % 3;
      }
      vectorCopy = BaseVector::copy(*vector, pool_.get());
      testPartitionedVector(vectorCopy, partitions, 4);
    }

    // Test with one value per partition
    if (vector->size() > 0) {
      std::iota(partitions.begin(), partitions.end(), 0);
      vectorCopy = BaseVector::copy(*vector, pool_.get());
      testPartitionedVector(vectorCopy, partitions, numRows);
    }

    // Test with random partitions (number of partitions <= number of values)
    std::uniform_int_distribution<> dis(0, numRows - 1);
    uint32_t maxPartition = 0;
    for (uint32_t i = 0; i < numRows; ++i) {
      partitions[i] = dis(gen_);
      maxPartition = std::max(maxPartition, partitions[i]);
    }
    vectorCopy = BaseVector::copy(*vector, pool_.get());
    testPartitionedVector(vectorCopy, partitions, maxPartition + 1);
  }
};

TEST_P(PartitioningVectorTest, testFlatVector) {
  // Number of values in the vector to be partitioned. This is passed as a test
  // parameter and is used to test different vector sizes, including edge cases
  // like 0 and 1.
  const int numValues = GetParam();

  // Random values, no nulls
  testVectorPartitioning(
      makeFlatVector<int>(numValues, [](auto row) { return row; }));

  // Random values, with half number of nulls
  testVectorPartitioning(
      makeFlatVector<int>(
          numValues, [](auto row) { return row; }, nullEvery(2, 1)));

  // All nulls
  testVectorPartitioning(makeAllNullFlatVector<int>(numValues));
}

TEST_P(PartitioningVectorTest, testFlatBoolVector) {
  const int numValues = GetParam();

  // Random values, no nulls
  testVectorPartitioning(
      makeFlatVector<bool>(numValues, [](auto row) { return row % 2 == 0; }));

  // Random values, with half number of nulls
  testVectorPartitioning(
      makeFlatVector<bool>(
          numValues, [](auto row) { return row % 2 == 0; }, nullEvery(2, 1)));

  // All nulls
  testVectorPartitioning(makeAllNullFlatVector<bool>(numValues));
}

TEST_P(PartitioningVectorTest, testRowVector) {
  const int numValues = GetParam();

  // Two flat columns, no nulls at any level.
  testVectorPartitioning(makeRowVector({
      makeFlatVector<int32_t>(numValues, [](auto row) { return row; }),
      makeFlatVector<int64_t>(numValues, [](auto row) { return row * 10; }),
  }));

  // Two flat columns with nullable children.
  testVectorPartitioning(makeRowVector({
      makeFlatVector<int32_t>(
          numValues, [](auto row) { return row; }, nullEvery(2)),
      makeFlatVector<int64_t>(
          numValues, [](auto row) { return row * 10; }, nullEvery(3)),
  }));

  // Row-level nulls with no child nulls.
  testVectorPartitioning(makeRowVector(
      {makeFlatVector<int32_t>(numValues, [](auto row) { return row; })},
      nullEvery(2)));

  // Row-level nulls combined with nullable children.
  testVectorPartitioning(makeRowVector(
      {makeFlatVector<int32_t>(
          numValues, [](auto row) { return row; }, nullEvery(3))},
      nullEvery(2)));

  // All rows null.
  testVectorPartitioning(makeRowVector(
      {makeFlatVector<int32_t>(numValues, [](auto row) { return row; })},
      [](auto /*row*/) { return true; }));

  // Nested RowVector, no nulls at any level.
  testVectorPartitioning(makeRowVector({
      makeFlatVector<int32_t>(numValues, [](auto row) { return row; }),
      makeRowVector({
          makeFlatVector<int64_t>(numValues, [](auto row) { return row; }),
      }),
  }));

  // Nested RowVector with row-level nulls on the outer row.
  testVectorPartitioning(makeRowVector(
      {makeRowVector({
          makeFlatVector<int32_t>(numValues, [](auto row) { return row; }),
      })},
      nullEvery(2)));

  // Nested RowVector with row-level nulls on the inner row.
  testVectorPartitioning(makeRowVector({
      makeRowVector(
          {makeFlatVector<int32_t>(numValues, [](auto row) { return row; })},
          nullEvery(3)),
  }));

  // Nested RowVector with row-level nulls at both levels.
  testVectorPartitioning(makeRowVector(
      {makeRowVector(
          {makeFlatVector<int32_t>(numValues, [](auto row) { return row; })},
          nullEvery(3))},
      nullEvery(2)));

  // Three-level nesting with nulls at every level.
  testVectorPartitioning(makeRowVector(
      {makeRowVector(
          {makeRowVector(
              {makeFlatVector<int32_t>(
                  numValues, [](auto row) { return row; })},
              nullEvery(4))},
          nullEvery(3))},
      nullEvery(2)));
}

TEST_P(PartitioningVectorTest, testConstantVector) {
  const int numValues = GetParam();

  testVectorPartitioning(makeConstant<int32_t>(7, numValues));
  testVectorPartitioning(makeConstant<int32_t>(std::nullopt, numValues));
  testVectorPartitioning(makeConstantRow(
      ROW({"c0", "c1"}, {INTEGER(), VARCHAR()}),
      variant::row({variant(11), variant("constant")}),
      numValues));
}

TEST_P(PartitioningVectorTest, singlePartitionWithoutAssignments) {
  const int numValues = GetParam();
  if (numValues == 0) {
    return;
  }

  auto row = makeRowVector(
      {makeFlatVector<int32_t>(numValues, [](auto row) { return row; }),
       makeFlatVector<bool>(numValues, [](auto row) { return row % 2 == 0; })});
  auto expected = BaseVector::copy(*row, pool_.get());

  auto partitionedVector = PartitionedVector::create(
      row,
      /*singlePartition=*/2u,
      4,
      ctx_,
      pool_.get());

  EXPECT_EQ(partitionedVector->rawPartitionOffsets()[0], 0);
  EXPECT_EQ(partitionedVector->rawPartitionOffsets()[1], 0);
  EXPECT_EQ(partitionedVector->rawPartitionOffsets()[2], numValues);
  EXPECT_EQ(partitionedVector->rawPartitionOffsets()[3], numValues);

  EXPECT_EQ(partitionedVector->partitionAt(0)->size(), 0);
  EXPECT_EQ(partitionedVector->partitionAt(1)->size(), 0);
  test::assertEqualVectors(
      expected, canonicalize(partitionedVector->partitionAt(2)));
  EXPECT_EQ(partitionedVector->partitionAt(3)->size(), 0);

  EXPECT_EQ(partitionedVector->numNullsAt(2), 0);

  auto nullableFlat = makeFlatVector<int32_t>(
      numValues, [](auto row) { return row; }, nullEvery(3));
  auto partitionedNullableFlat = PartitionedVector::create(
      nullableFlat,
      /*singlePartition=*/2u,
      4,
      ctx_,
      pool_.get());

  EXPECT_EQ(
      partitionedNullableFlat->numNullsAt(2),
      BaseVector::countNulls(nullableFlat->nulls(), 0, numValues));
}

TEST_P(PartitioningVectorTest, testDictionaryVector) {
  const int numValues = GetParam();

  auto base = makeFlatVector<int32_t>(
      numValues, [](auto row) { return row / 2; }, nullEvery(3));
  auto indices = makeIndices(
      numValues, [numValues](auto row) { return numValues - row - 1; });
  auto nulls = makeNulls(numValues, [](auto row) { return row % 4 == 0; });
  auto dictionary =
      BaseVector::wrapInDictionary(nulls, indices, numValues, std::move(base));

  testVectorPartitioning(dictionary);

  auto sharedIndices = makeIndices(
      numValues, [numValues](auto row) { return numValues - row - 1; });
  auto row = makeRowVector({
      BaseVector::wrapInDictionary(
          nullptr,
          sharedIndices,
          numValues,
          makeFlatVector<int32_t>(numValues, [](auto row) { return row; })),
      BaseVector::wrapInDictionary(
          nullptr,
          sharedIndices,
          numValues,
          makeFlatVector<int64_t>(
              numValues, [](auto row) { return row * 10; })),
  });
  std::vector<uint32_t> partitions(numValues);
  for (vector_size_t row = 0; row < numValues; ++row) {
    partitions[row] = row % 2;
  }
  testPartitionedVector(row, partitions, 2);
}

TEST_P(PartitioningVectorTest, testDictionaryEncodedRowVector) {
  const int numValues = GetParam();

  auto row = makeRowVector(
      {makeFlatVector<int32_t>(
           numValues, [](auto row) { return row; }, nullEvery(3)),
       makeFlatVector<int64_t>(numValues, [](auto row) { return row * 10; })},
      nullEvery(5));
  auto indices = makeIndices(
      numValues, [numValues](auto row) { return numValues - row - 1; });
  auto nulls = makeNulls(numValues, [](auto row) { return row % 4 == 0; });
  auto dictionary =
      BaseVector::wrapInDictionary(nulls, indices, numValues, row);

  testVectorPartitioning(dictionary);
}

TEST_P(PartitioningVectorTest, testNestedDictionaryVector) {
  const int numValues = GetParam();

  auto base = makeFlatVector<int32_t>(
      numValues, [](auto row) { return row; }, nullEvery(5));
  auto inner = BaseVector::wrapInDictionary(
      makeNulls(numValues, [](auto row) { return row % 6 == 0; }),
      makeIndices(numValues, [numValues](auto row) { return row % numValues; }),
      numValues,
      base);
  auto outer = BaseVector::wrapInDictionary(
      makeNulls(numValues, [](auto row) { return row % 4 == 0; }),
      makeIndices(
          numValues, [numValues](auto row) { return numValues - row - 1; }),
      numValues,
      inner);

  testVectorPartitioning(outer);
}

// Partitioning a null-free vector must not allocate a null buffer.
TEST_P(PartitioningVectorTest, noNullBufferAllocatedForNullFreeFlat) {
  const int numValues = GetParam();
  if (numValues == 0) {
    return;
  }

  auto flat = makeFlatVector<int32_t>(numValues, [](auto row) { return row; });
  ASSERT_FALSE(flat->mayHaveNulls());

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 2;
  }

  auto pv = PartitionedVector::create(flat, partitions, 2, ctx_, pool_.get());
  EXPECT_FALSE(pv->baseVector()->mayHaveNulls())
      << "partition() must not allocate a null buffer for a null-free FlatVector";
}

// Partitioning a null-free RowVector must not allocate null buffers on the
// row vector or any of its children.
TEST_P(PartitioningVectorTest, noNullBufferAllocatedForNullFreeRow) {
  const int numValues = GetParam();
  if (numValues == 0) {
    return;
  }

  auto row = makeRowVector({
      makeFlatVector<int32_t>(numValues, [](auto row) { return row; }),
      makeFlatVector<int64_t>(numValues, [](auto row) { return row * 10; }),
  });
  ASSERT_FALSE(row->mayHaveNulls());
  ASSERT_FALSE(row->childAt(0)->mayHaveNulls());
  ASSERT_FALSE(row->childAt(1)->mayHaveNulls());

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 2;
  }

  auto pv = PartitionedVector::create(row, partitions, 2, ctx_, pool_.get());
  auto* base = pv->baseVector()->as<RowVector>();
  EXPECT_FALSE(base->mayHaveNulls())
      << "partition() must not allocate a null buffer for a null-free RowVector";
  EXPECT_FALSE(base->childAt(0)->mayHaveNulls())
      << "partition() must not allocate a null buffer for null-free child 0";
  EXPECT_FALSE(base->childAt(1)->mayHaveNulls())
      << "partition() must not allocate a null buffer for null-free child 1";
}

// numNullsAt() tests
// ---------------------------------------------------------------------------

// A null-free flat vector must report zero nulls for every partition.
TEST_P(PartitioningVectorTest, numNullsAtFlatNoNulls) {
  const int numValues = GetParam();
  auto flat = makeFlatVector<int32_t>(numValues, [](auto row) { return row; });

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 3;
  }
  auto pv = PartitionedVector::create(flat, partitions, 3, ctx_, pool_.get());
  for (uint32_t p = 0; p < 3; ++p) {
    EXPECT_EQ(pv->numNullsAt(p), 0) << "partition " << p;
  }
}

// A flat vector with every other row null must report the exact per-partition
// null count. The sum across all partitions must equal the total null count.
TEST_P(PartitioningVectorTest, numNullsAtFlatSomeNulls) {
  const int numValues = GetParam();
  auto flat = makeFlatVector<int32_t>(
      numValues, [](auto row) { return row; }, nullEvery(2));

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 3;
  }
  auto pv = PartitionedVector::create(flat, partitions, 3, ctx_, pool_.get());

  // Per-partition counts must agree with manual bit-scan of the base vector.
  const auto* rawNulls = pv->baseVector()->rawNulls();
  const auto* rawOffsets = pv->rawPartitionOffsets();
  for (uint32_t p = 0; p < 3; ++p) {
    const vector_size_t begin = p == 0 ? 0 : rawOffsets[p - 1];
    const vector_size_t end = rawOffsets[p];
    const vector_size_t expected = rawNulls
        ? BaseVector::countNulls(pv->baseVector()->nulls(), begin, end)
        : 0;
    EXPECT_EQ(pv->numNullsAt(p), expected) << "partition " << p;
  }

  // Sum across partitions must equal the total null count in the source vector.
  const vector_size_t total =
      pv->numNullsAt(0) + pv->numNullsAt(1) + pv->numNullsAt(2);
  EXPECT_EQ(total, BaseVector::countNulls(flat->nulls(), 0, numValues));
}

// An all-null flat vector must report numNullsAt(p) == rows in that partition.
TEST_P(PartitioningVectorTest, numNullsAtFlatAllNulls) {
  const int numValues = GetParam();
  auto flat = makeAllNullFlatVector<int32_t>(numValues);

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 3;
  }
  auto pv = PartitionedVector::create(flat, partitions, 3, ctx_, pool_.get());

  const auto* rawOffsets = pv->rawPartitionOffsets();
  for (uint32_t p = 0; p < 3; ++p) {
    const vector_size_t begin = p == 0 ? 0 : rawOffsets[p - 1];
    const vector_size_t numRowsInPartition = rawOffsets[p] - begin;
    EXPECT_EQ(pv->numNullsAt(p), numRowsInPartition) << "partition " << p;
  }
}

TEST_P(PartitioningVectorTest, numNullsAtDictionary) {
  const int numValues = GetParam();
  auto base = makeFlatVector<int32_t>(
      numValues, [](auto row) { return row; }, nullEvery(3));
  auto dictionary = BaseVector::wrapInDictionary(
      makeNulls(numValues, [](auto row) { return row % 4 == 0; }),
      makeIndices(
          numValues, [numValues](auto row) { return numValues - row - 1; }),
      numValues,
      base);

  std::vector<uint32_t> partitions(numValues);
  std::vector<vector_size_t> expected(3, 0);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 3;
    expected[partitions[i]] += dictionary->isNullAt(i);
  }

  auto pv =
      PartitionedVector::create(dictionary, partitions, 3, ctx_, pool_.get());
  for (uint32_t p = 0; p < 3; ++p) {
    EXPECT_EQ(pv->numNullsAt(p), expected[p]) << "partition " << p;
  }
}

// A row vector with no row-level nulls must report zero per-partition nulls at
// the row level, even when child columns have nulls.
TEST_P(PartitioningVectorTest, numNullsAtRowNoRowLevelNulls) {
  const int numValues = GetParam();
  auto row = makeRowVector({
      makeFlatVector<int32_t>(
          numValues, [](auto row) { return row; }, nullEvery(2)),
  });
  ASSERT_FALSE(row->mayHaveNulls());

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 3;
  }
  auto pv = PartitionedVector::create(row, partitions, 3, ctx_, pool_.get());
  for (uint32_t p = 0; p < 3; ++p) {
    EXPECT_EQ(pv->numNullsAt(p), 0)
        << "Row-level numNullsAt() must not count child nulls, partition " << p;
  }
}

// A row vector with row-level nulls must report per-partition counts that match
// a manual bit-scan. Child null counts must be counted independently.
TEST_P(PartitioningVectorTest, numNullsAtRowRowLevelNulls) {
  const int numValues = GetParam();
  auto row = makeRowVector(
      {makeFlatVector<int32_t>(
          numValues, [](auto row) { return row; }, nullEvery(3))},
      nullEvery(2));

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 3;
  }
  auto pv = PartitionedVector::create(row, partitions, 3, ctx_, pool_.get());

  const auto* rawOffsets = pv->rawPartitionOffsets();
  for (uint32_t p = 0; p < 3; ++p) {
    const vector_size_t begin = p == 0 ? 0 : rawOffsets[p - 1];
    const vector_size_t end = rawOffsets[p];
    const vector_size_t expected =
        BaseVector::countNulls(pv->baseVector()->nulls(), begin, end);
    EXPECT_EQ(pv->numNullsAt(p), expected)
        << "Row-level null count mismatch, partition " << p;
  }

  // Child null counts must be tracked independently of row-level nulls.
  auto* prv = dynamic_cast<PartitionedRowVector*>(pv.get());
  ASSERT_NE(prv, nullptr);
  auto child = prv->childAt(0);
  const auto* childOffsets = child->rawPartitionOffsets();
  for (uint32_t p = 0; p < 3; ++p) {
    const vector_size_t begin = p == 0 ? 0 : childOffsets[p - 1];
    const vector_size_t end = childOffsets[p];
    const vector_size_t expected =
        BaseVector::countNulls(child->baseVector()->nulls(), begin, end);
    EXPECT_EQ(child->numNullsAt(p), expected)
        << "Child null count mismatch, partition " << p;
  }
}

// numNullsAt() for a nested RowVector must track outer and inner null counts
// independently at every level of nesting.
TEST_P(PartitioningVectorTest, numNullsAtNestedRow) {
  const int numValues = 10;

  // Outer row-level nulls on every even row; inner child has no nulls.
  auto inner = makeRowVector({
      makeFlatVector<int32_t>(
          numValues, [](auto row) { return row; }, nullEvery(3)),
  });
  auto outer = makeRowVector({inner}, nullEvery(2));

  std::vector<uint32_t> partitions(numValues);
  for (int i = 0; i < numValues; ++i) {
    partitions[i] = i % 3;
  }
  auto pv = PartitionedVector::create(outer, partitions, 3, ctx_, pool_.get());

  // Outer null counts must match a manual bit-scan of the outer null bitmap.
  const auto* rawOffsets = pv->rawPartitionOffsets();
  for (uint32_t p = 0; p < 3; ++p) {
    const vector_size_t begin = p == 0 ? 0 : rawOffsets[p - 1];
    const vector_size_t end = rawOffsets[p];
    const vector_size_t expected =
        BaseVector::countNulls(pv->baseVector()->nulls(), begin, end);
    EXPECT_EQ(pv->numNullsAt(p), expected)
        << "outer null count mismatch, partition " << p;
  }

  // Inner RowVector child must report zero nulls (it has no null bitmap).
  auto* prv = dynamic_cast<PartitionedRowVector*>(pv.get());
  ASSERT_NE(prv, nullptr);
  auto innerPv = prv->childAt(0);
  for (uint32_t p = 0; p < 3; ++p) {
    EXPECT_EQ(innerPv->numNullsAt(p), 0)
        << "inner null count must be zero when inner has no nulls, partition "
        << p;
  }

  // Repeat with nulls on both the outer and inner RowVector.
  auto innerWithNulls = makeRowVector(
      {makeFlatVector<int32_t>(numValues, [](auto row) { return row; })},
      nullEvery(3));
  auto outerWithNulls =
      makeRowVector({BaseVector::copy(*innerWithNulls)}, nullEvery(2));

  auto pv2 = PartitionedVector::create(
      outerWithNulls, partitions, 3, ctx_, pool_.get());

  const auto* rawOffsets2 = pv2->rawPartitionOffsets();
  auto* prv2 = dynamic_cast<PartitionedRowVector*>(pv2.get());
  ASSERT_NE(prv2, nullptr);
  auto innerPv2 = prv2->childAt(0);
  const auto* innerOffsets = innerPv2->rawPartitionOffsets();

  for (uint32_t p = 0; p < 3; ++p) {
    // Outer nulls.
    const vector_size_t outerBegin = p == 0 ? 0 : rawOffsets2[p - 1];
    const vector_size_t outerEnd = rawOffsets2[p];
    const vector_size_t expectedOuter = BaseVector::countNulls(
        pv2->baseVector()->nulls(), outerBegin, outerEnd);
    EXPECT_EQ(pv2->numNullsAt(p), expectedOuter)
        << "outer null count mismatch (both levels), partition " << p;

    // Inner nulls must be tracked independently of the outer nulls.
    const vector_size_t innerBegin = p == 0 ? 0 : innerOffsets[p - 1];
    const vector_size_t innerEnd = innerOffsets[p];
    const vector_size_t expectedInner = BaseVector::countNulls(
        innerPv2->baseVector()->nulls(), innerBegin, innerEnd);
    EXPECT_EQ(innerPv2->numNullsAt(p), expectedInner)
        << "inner null count mismatch (both levels), partition " << p;
  }
}

TEST_F(PartitioningVectorTest, reusePartitionedDictionaryIndices) {
  constexpr vector_size_t kNumValues{8};
  auto sharedIndices =
      makeIndices(kNumValues, [](auto row) { return kNumValues - row - 1; });
  auto row = makeRowVector({
      BaseVector::wrapInDictionary(
          nullptr,
          sharedIndices,
          kNumValues,
          makeFlatVector<int32_t>(kNumValues, [](auto row) { return row; })),
      BaseVector::wrapInDictionary(
          nullptr,
          sharedIndices,
          kNumValues,
          makeFlatVector<int64_t>(
              kNumValues, [](auto row) { return row * 10; })),
  });

  const std::vector<uint32_t> partitions{0, 1, 0, 1, 0, 1, 0, 1};
  auto partitionedVector =
      PartitionedVector::create(row, partitions, 2, ctx_, pool_.get());

  auto* partitionedRow = partitionedVector->baseVector()->as<RowVector>();
  ASSERT_NE(partitionedRow, nullptr);
  auto* first = partitionedRow->childAt(0)->as<DictionaryVector<int32_t>>();
  auto* second = partitionedRow->childAt(1)->as<DictionaryVector<int64_t>>();
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(ctx_.partitionedDictionaryIndices.size(), 1);
  EXPECT_EQ(first->indices(), second->indices());
  EXPECT_NE(first->indices(), sharedIndices);
}

TEST_F(PartitioningVectorTest, aliasedChildrenPartitionedOnce) {
  constexpr vector_size_t kNumValues{8};
  auto shared = makeNullableFlatVector<int64_t>(
      {0, 10, std::nullopt, 30, 40, std::nullopt, 60, 70});
  auto row = makeRowVector({
      makeFlatVector<int64_t>(kNumValues, [](auto row) { return row; }),
      shared,
      shared,
  });

  const std::vector<uint32_t> partitions{0, 1, 0, 1, 0, 1, 0, 1};
  auto partitionedVector =
      PartitionedVector::create(row, partitions, 2, ctx_, pool_.get());

  auto* partitionedRow = partitionedVector->as<PartitionedRowVector>();
  ASSERT_NE(partitionedRow, nullptr);
  EXPECT_EQ(partitionedRow->childAt(1), partitionedRow->childAt(2));

  auto expectedP0 = makeRowVector({
      makeFlatVector<int64_t>({0, 2, 4, 6}),
      makeNullableFlatVector<int64_t>({0, std::nullopt, 40, 60}),
      makeNullableFlatVector<int64_t>({0, std::nullopt, 40, 60}),
  });
  auto expectedP1 = makeRowVector({
      makeFlatVector<int64_t>({1, 3, 5, 7}),
      makeNullableFlatVector<int64_t>({10, 30, std::nullopt, 70}),
      makeNullableFlatVector<int64_t>({10, 30, std::nullopt, 70}),
  });
  test::assertEqualVectors(expectedP0, partitionedVector->partitionAt(0));
  test::assertEqualVectors(expectedP1, partitionedVector->partitionAt(1));
}

// Test with different vector sizes, including edge cases like 0 and 1.
INSTANTIATE_TEST_SUITE_P(
    FlatVectorSizes,
    PartitioningVectorTest,
    ::testing::Values(0, 1, 10, 10000));

} // namespace facebook::velox::test
