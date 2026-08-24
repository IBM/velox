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

#include <random>
#include <string>
#include <string_view>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/serializers/PrestoIterativePartitioningSerializer.h"

#include "velox/serializers/PrestoSerializerSerializationUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::serializer::presto;
using namespace facebook::velox::test;

namespace {

int64_t simpleColumnPageBytes(
    std::string_view encodingName,
    int64_t numRows,
    int64_t numNulls,
    int64_t valueWidth) {
  return serializer::presto::detail::kHeaderSize + 4 // page header + num cols
      + 4 + static_cast<int64_t>(encodingName.size()) // column header
      + 4 // num rows
      + 1 + (numNulls > 0 ? bits::nbytes(numRows) : 0) // null flags
      + (numRows - numNulls) * valueWidth; // values
}

} // namespace

// ---------------------------------------------------------------------------
// Shared base fixture
// ---------------------------------------------------------------------------

class PrestoIterativePartitioningSerializerTestBase : public VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    if (!isRegisteredVectorSerde()) {
      PrestoVectorSerde::registerVectorSerde();
    }
  }

  /// Deserializes an IOBuf produced by PartitioningSerializer::flush().
  RowVectorPtr deserialize(folly::IOBuf& iobuf, const RowTypePtr& type) {
    auto ranges = byteRangesFromIOBuf(&iobuf);
    BufferInputStream stream(std::move(ranges));
    RowVectorPtr result;
    serde_.deserialize(&stream, pool_.get(), type, &result, nullptr);
    return result;
  }

  VectorPtr canonicalize(VectorPtr vector) {
    auto indices = makeIndices(vector->size(), [](auto row) { return row; });
    auto* rawIndices = indices->asMutable<vector_size_t>();
    std::stable_sort(
        rawIndices,
        rawIndices + vector->size(),
        [&](vector_size_t left, vector_size_t right) {
          return vector->compare(vector.get(), left, right) < 0;
        });
    return BaseVector::wrapInDictionary(
        nullptr, indices, vector->size(), vector);
  }

  /// Extracts flat values from a column into a sorted vector.
  template <typename T>
  std::vector<T> sortedValues(const RowVectorPtr& row, int column) {
    auto* flat = row->childAt(column)->as<FlatVector<T>>();
    std::vector<T> vals(flat->rawValues(), flat->rawValues() + row->size());
    std::sort(vals.begin(), vals.end());
    return vals;
  }

  /// Extracts values from a nullable column, preserving order and nulls.
  template <typename T>
  std::vector<std::optional<T>> nullableValues(
      const RowVectorPtr& row,
      int column) {
    auto* vec = row->childAt(column).get();
    std::vector<std::optional<T>> result;
    result.reserve(row->size());
    for (int i = 0; i < row->size(); ++i) {
      if (vec->isNullAt(i)) {
        result.push_back(std::nullopt);
      } else {
        result.push_back(vec->as<FlatVector<T>>()->valueAt(i));
      }
    }
    return result;
  }

  /// Extracts values from a nullable column into a sorted vector.
  template <typename T>
  std::vector<std::optional<T>> sortedNullableValues(
      const RowVectorPtr& row,
      int column) {
    auto result = nullableValues<T>(row, column);
    std::sort(result.begin(), result.end());
    return result;
  }

  /// Builds a PrestoIterativePartitioningSerializer with default serde options.
  std::unique_ptr<PrestoIterativePartitioningSerializer> makeSerializer(
      const RowTypePtr& type,
      uint32_t numPartitions) {
    SerdeOpts opts;
    return std::make_unique<PrestoIterativePartitioningSerializer>(
        type,
        numPartitions,
        opts,
        pool_.get(),
        []() -> std::unique_ptr<OutputStreamListener> {
          return std::make_unique<PrestoOutputStreamListener>();
        });
  }

  /// Builds a serializer that computes a CRC32 checksum on each flush via a
  /// PrestoOutputStreamListener factory, matching the kOptimized path when
  /// OutputBufferManager has a listener factory set.
  std::unique_ptr<PrestoIterativePartitioningSerializer>
  makeSerializerWithListener(const RowTypePtr& type, uint32_t numPartitions) {
    SerdeOpts opts;
    return std::make_unique<PrestoIterativePartitioningSerializer>(
        type,
        numPartitions,
        opts,
        pool_.get(),
        []() -> std::unique_ptr<OutputStreamListener> {
          return std::make_unique<PrestoOutputStreamListener>();
        });
  }

  // Presto page header layout: [numRows:4][codec:1][uncompressedSize:4]
  //                             [compressedSize:4][checksum:8]
  static constexpr int kCodecByteOffset = 4;
  static constexpr int kChecksumOffset = 13;
  static constexpr int8_t kChecksumBitMask = 4;

  /// Returns the codec byte from the Presto page header in `iobuf`.
  static int8_t codecByte(const folly::IOBuf& iobuf) {
    VELOX_CHECK_GE(iobuf.length(), kChecksumOffset + 8);
    return reinterpret_cast<const int8_t*>(iobuf.data())[kCodecByteOffset];
  }

  /// Returns the 8-byte checksum field from the Presto page header in `iobuf`.
  static int64_t checksumField(const folly::IOBuf& iobuf) {
    VELOX_CHECK_GE(iobuf.length(), kChecksumOffset + 8);
    int64_t value;
    std::memcpy(&value, iobuf.data() + kChecksumOffset, sizeof(value));
    return value;
  }

  int64_t totalFlushedBytes(
      std::map<
          uint32_t,
          std::pair<std::unique_ptr<folly::IOBuf>, vector_size_t>>& pages)
      const {
    int64_t totalBytes = 0;
    for (const auto& [_, page] : pages) {
      totalBytes += page.first->computeChainDataLength();
    }
    return totalBytes;
  }

  PrestoVectorSerde serde_;
};

template <>
std::vector<bool> PrestoIterativePartitioningSerializerTestBase::sortedValues<
    bool>(const RowVectorPtr& row, int column) {
  auto* flat = row->childAt(column)->as<FlatVector<bool>>();
  std::vector<bool> vals;
  vals.reserve(row->size());
  for (int i = 0; i < row->size(); ++i) {
    vals.push_back(flat->valueAtFast(i));
  }
  std::sort(vals.begin(), vals.end());
  return vals;
}

// ---------------------------------------------------------------------------
// Value-parameterized fixture — routing, null-handling over scalar TypePtrs.
// Uses BaseVector::create() + setNull() so no C++ type dispatch is needed.
// ---------------------------------------------------------------------------

class PrestoIterativePartitioningSerializerParamTest
    : public ::testing::TestWithParam<TypePtr>,
      public PrestoIterativePartitioningSerializerTestBase {
 public:
  static void SetUpTestSuite() {
    PrestoIterativePartitioningSerializerTestBase::SetUpTestSuite();
  }
};

// Short lowercase names for test output, matching the benchmark convention.
std::string scalarTypeName(const TypePtr& type) {
  if (type->kind() == TypeKind::BOOLEAN)
    return "bool";
  if (type->kind() == TypeKind::INTEGER)
    return "int";
  if (type->kind() == TypeKind::BIGINT)
    return "bigint";
  if (type->kind() == TypeKind::HUGEINT)
    return "hugeint";
  return type->toString();
}

INSTANTIATE_TEST_SUITE_P(
    ScalarTypes,
    PrestoIterativePartitioningSerializerParamTest,
    ::testing::Values(BOOLEAN(), INTEGER(), BIGINT(), HUGEINT()),
    [](const ::testing::TestParamInfo<TypePtr>& info) {
      return scalarTypeName(info.param);
    });

// ── Routing ──────────────────────────────────────────────────────────────────

// Single append, two equal-sized partitions; also verifies rowsBuffered and
// bytesBuffered lifecycle counters.
TEST_P(PrestoIterativePartitioningSerializerParamTest, basicTwoPartitions) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto col = BaseVector::create(colType, 6, pool_.get());
  auto input = makeRowVector({"a"}, {col});

  // Even rows → partition 0, odd rows → partition 1.
  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 1, 0, 1, 0, 1});

  EXPECT_EQ(serializer->rowsBuffered(), 6);

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  EXPECT_EQ(serializer->rowsBuffered(), 0);
  EXPECT_EQ(serializer->bytesBuffered(), 0);

  auto p0 = deserialize(*ioBufs.at(0).first, type);
  auto p1 = deserialize(*ioBufs.at(1).first, type);

  EXPECT_EQ(p0->size(), 3);
  EXPECT_EQ(p1->size(), 3);
}

// All rows routed to one non-zero partition; other partitions are absent.
TEST_P(PrestoIterativePartitioningSerializerParamTest, allRowsToOnePartition) {
  auto colType = GetParam();
  auto type = ROW({"x"}, {colType});
  auto col = BaseVector::create(colType, 5, pool_.get());
  auto input = makeRowVector({"x"}, {col});

  auto serializer = makeSerializer(type, 4);
  serializer->append(input, {2, 2, 2, 2, 2});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 1);
  ASSERT_TRUE(ioBufs.count(2));
  EXPECT_EQ(deserialize(*ioBufs.at(2).first, type)->size(), 5);
}

// All rows routed to one non-zero partition without materialized assignments.
TEST_P(
    PrestoIterativePartitioningSerializerParamTest,
    allRowsToOnePartitionFastPath) {
  auto colType = GetParam();
  auto type = ROW({"x"}, {colType});
  auto col = BaseVector::create(colType, 5, pool_.get());
  auto input = makeRowVector({"x"}, {col});

  auto serializer = makeSerializer(type, 4);
  serializer->append(input, /*singlePartition=*/2);
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 1);
  ASSERT_TRUE(ioBufs.count(2));
  EXPECT_EQ(deserialize(*ioBufs.at(2).first, type)->size(), 5);
}

// Single partition (numPartitions=1): all rows go to partition 0.
TEST_P(PrestoIterativePartitioningSerializerParamTest, singlePartition) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto col = BaseVector::create(colType, 5, pool_.get());
  auto input = makeRowVector({"a"}, {col});

  auto serializer = makeSerializer(type, 1);
  serializer->append(input, std::vector<uint32_t>(5, 0));
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 1);
  EXPECT_EQ(deserialize(*ioBufs.at(0).first, type)->size(), 5);
}

// Multiple columns of the same type: each is serialized independently by
// flushRowChildren.
TEST_P(PrestoIterativePartitioningSerializerParamTest, multipleColumns) {
  auto colType = GetParam();
  auto type = ROW({"a", "b"}, {colType, colType});
  auto colA = BaseVector::create(colType, 4, pool_.get());
  auto colB = BaseVector::create(colType, 4, pool_.get());
  auto input = makeRowVector({"a", "b"}, {colA, colB});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  EXPECT_EQ(r0->size(), 2);
  EXPECT_EQ(r0->childAt(0)->size(), 2);
  EXPECT_EQ(r0->childAt(1)->size(), 2);

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  EXPECT_EQ(r1->size(), 2);
  EXPECT_EQ(r1->childAt(0)->size(), 2);
  EXPECT_EQ(r1->childAt(1)->size(), 2);
}

// ── Null handling
// ─────────────────────────────────────────────────────────────

// Nulls appear only in one partition; the other partition is null-free.
// Rows 0,1,2 → p0; rows 3,4 → p1. Row 1 is null.
// p0: [not-null, null, not-null]; p1: [not-null, not-null].
TEST_P(PrestoIterativePartitioningSerializerParamTest, nullsInOnePartition) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto col = BaseVector::create(colType, 5, pool_.get());
  col->setNull(1, true);
  auto input = makeRowVector({"a"}, {col});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 3);
  EXPECT_FALSE(r0->childAt(0)->isNullAt(0));
  EXPECT_TRUE(r0->childAt(0)->isNullAt(1));
  EXPECT_FALSE(r0->childAt(0)->isNullAt(2));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_FALSE(r1->childAt(0)->isNullAt(0));
  EXPECT_FALSE(r1->childAt(0)->isNullAt(1));
}

// Nulls contributed by different appends to the same partition.
// Append 1: rows 0,1 → p0 (row 1 null); row 2 → p1.
// Append 2: row 0 → p0 (null); row 1 → p1.
// p0: [not-null, null, null]; p1: [not-null, not-null].
TEST_P(
    PrestoIterativePartitioningSerializerParamTest,
    nullsAcrossMultipleAppends) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto serializer = makeSerializer(type, 2);

  auto col1 = BaseVector::create(colType, 3, pool_.get());
  col1->setNull(1, true);
  serializer->append(makeRowVector({"a"}, {col1}), {0, 0, 1});

  auto col2 = BaseVector::create(colType, 2, pool_.get());
  col2->setNull(0, true);
  serializer->append(makeRowVector({"a"}, {col2}), {0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 3);
  EXPECT_FALSE(r0->childAt(0)->isNullAt(0));
  EXPECT_TRUE(r0->childAt(0)->isNullAt(1));
  EXPECT_TRUE(r0->childAt(0)->isNullAt(2));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_FALSE(r1->childAt(0)->isNullAt(0));
  EXPECT_FALSE(r1->childAt(0)->isNullAt(1));
}

// Partition boundary falls in the middle of a null-bitmap byte, exercising the
// bit-extraction carry-over logic. 5 rows → p0, 4 rows → p1. The boundary at
// bit 5 is inside the first byte of the null bitmap. Rows 1,3,5,7 are null.
// p0: [not-null, null, not-null, null, not-null].
// p1: [null, not-null, null, not-null].
TEST_P(PrestoIterativePartitioningSerializerParamTest, nullsUnalignedBoundary) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto col = BaseVector::create(colType, 9, pool_.get());
  col->setNull(1, true);
  col->setNull(3, true);
  col->setNull(5, true);
  col->setNull(7, true);
  auto input = makeRowVector({"a"}, {col});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 0, 0, 0, 1, 1, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 5);
  EXPECT_FALSE(r0->childAt(0)->isNullAt(0));
  EXPECT_TRUE(r0->childAt(0)->isNullAt(1));
  EXPECT_FALSE(r0->childAt(0)->isNullAt(2));
  EXPECT_TRUE(r0->childAt(0)->isNullAt(3));
  EXPECT_FALSE(r0->childAt(0)->isNullAt(4));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 4);
  EXPECT_TRUE(r1->childAt(0)->isNullAt(0));
  EXPECT_FALSE(r1->childAt(0)->isNullAt(1));
  EXPECT_TRUE(r1->childAt(0)->isNullAt(2));
  EXPECT_FALSE(r1->childAt(0)->isNullAt(3));
}

// Both partitions contain nulls.
// Input: 4 rows, rows 1 and 2 null; rows 0,1 → p0; rows 2,3 → p1.
// p0: [not-null, null]; p1: [null, not-null].
TEST_P(PrestoIterativePartitioningSerializerParamTest, nullsInBothPartitions) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto col = BaseVector::create(colType, 4, pool_.get());
  col->setNull(1, true);
  col->setNull(2, true);
  auto input = makeRowVector({"a"}, {col});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 2);
  EXPECT_FALSE(r0->childAt(0)->isNullAt(0));
  EXPECT_TRUE(r0->childAt(0)->isNullAt(1));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_TRUE(r1->childAt(0)->isNullAt(0));
  EXPECT_FALSE(r1->childAt(0)->isNullAt(1));
}

// All rows in one partition are null; the other partition is non-null.
// Input: 3 rows, rows 0,1 null; rows 0,1 → p0; row 2 → p1.
TEST_P(PrestoIterativePartitioningSerializerParamTest, allNullsInPartition) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto col = BaseVector::create(colType, 3, pool_.get());
  col->setNull(0, true);
  col->setNull(1, true);
  auto input = makeRowVector({"a"}, {col});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 2);
  EXPECT_TRUE(r0->childAt(0)->isNullAt(0));
  EXPECT_TRUE(r0->childAt(0)->isNullAt(1));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 1);
  EXPECT_FALSE(r1->childAt(0)->isNullAt(0));
}

// A null batch followed by a null-free batch for the same partition.
// Regression: bitmaps must be initialized to all-not-null so that rows from
// the null-free batch (rawNulls == nullptr) are not decoded as null.
TEST_P(
    PrestoIterativePartitioningSerializerParamTest,
    nullBatchFollowedByNullFreeBatch) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto serializer = makeSerializer(type, 2);

  // Append 1: row 0 → p0 (null); row 1 → p1 (not-null).  rawNulls non-null.
  auto col1 = BaseVector::create(colType, 2, pool_.get());
  col1->setNull(0, true);
  serializer->append(makeRowVector({"a"}, {col1}), {0, 1});

  // Append 2: all not-null (rawNulls == nullptr).  row 0 → p0; row 1 → p1.
  auto col2 = BaseVector::create(colType, 2, pool_.get());
  serializer->append(makeRowVector({"a"}, {col2}), {0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  // p0: [null (append 1), not-null (append 2)]
  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 2);
  EXPECT_TRUE(r0->childAt(0)->isNullAt(0));
  EXPECT_FALSE(r0->childAt(0)->isNullAt(1));

  // p1: [not-null (append 1), not-null (append 2)]
  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_FALSE(r1->childAt(0)->isNullAt(0));
  EXPECT_FALSE(r1->childAt(0)->isNullAt(1));
}

// Nested RowVector.
// Round-trips 1-, 2-, and 3-level nested ROW columns through append() +
// flush() + deserialize(). Rows are routed across two partitions; the leaf
// integer values must survive the round trip at every nesting level.
class PrestoIterativePartitioningSerializerNestedRowTest
    : public ::testing::TestWithParam<int>,
      public PrestoIterativePartitioningSerializerTestBase {
 public:
  static void SetUpTestSuite() {
    PrestoIterativePartitioningSerializerTestBase::SetUpTestSuite();
  }

 protected:
  // Builds an n-level nested ROW type. Level 1 is ROW(int); each additional
  // level wraps the previous type as the only child of a new ROW.
  RowTypePtr makeNestedRowType(int level) {
    RowTypePtr type = ROW({"a"}, {INTEGER()});
    for (int i = 1; i < level; ++i) {
      type = ROW({"r"}, {type});
    }
    return type;
  }

  // Builds an input RowVector with leaf values [0, numRows). Each level wraps
  // the previous one as a single-field ROW; no nulls are introduced.
  RowVectorPtr makeNestedInput(int level, int numRows) {
    VectorPtr current =
        makeFlatVector<int32_t>(numRows, [](auto row) { return row; });
    for (int depth = 0; depth < level; ++depth) {
      current = makeRowVector({depth == 0 ? "a" : "r"}, {current});
    }
    return std::dynamic_pointer_cast<RowVector>(current);
  }

  // Walks from the outer page down to the leaf, returning the leaf int32 value
  // at j. Asserts that no row is null at any level.
  int32_t leafValueAt(const RowVectorPtr& page, int j, int level) {
    const RowVector* row = page.get();
    for (int outer = level - 1; outer > 0; --outer) {
      EXPECT_FALSE(row->isNullAt(j))
          << "outer null at depth " << outer << " row " << j;
      row = row->childAt(0)->as<RowVector>();
    }
    EXPECT_FALSE(row->isNullAt(j)) << "innermost null at row " << j;
    return row->childAt(0)->as<FlatVector<int32_t>>()->valueAt(j);
  }
};

TEST_P(PrestoIterativePartitioningSerializerNestedRowTest, roundTrip) {
  const int level = GetParam();
  constexpr int kNumRows = 24;

  auto type = makeNestedRowType(level);
  auto input = makeNestedInput(level, kNumRows);

  std::vector<uint32_t> partitions(kNumRows);
  for (int i = 0; i < kNumRows; ++i) {
    partitions[i] = i % 2;
  }

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, partitions);
  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  for (int parity = 0; parity < 2; ++parity) {
    auto page = deserialize(*ioBufs.at(parity).first, type);
    ASSERT_EQ(page->size(), kNumRows / 2) << "parity " << parity;

    std::vector<int32_t> values;
    values.reserve(page->size());
    for (int j = 0; j < page->size(); ++j) {
      values.push_back(leafValueAt(page, j, level));
    }
    std::sort(values.begin(), values.end());

    std::vector<int32_t> expected;
    for (int i = parity; i < kNumRows; i += 2) {
      expected.push_back(i);
    }
    EXPECT_EQ(values, expected);
  }
}

class PrestoIterativePartitioningSerializerNestedRowNullTest
    : public ::testing::Test,
      public PrestoIterativePartitioningSerializerTestBase {
 public:
  static void SetUpTestSuite() {
    PrestoIterativePartitioningSerializerTestBase::SetUpTestSuite();
  }
};

TEST_F(PrestoIterativePartitioningSerializerNestedRowNullTest, roundTrip) {
  constexpr int kNumRows = 12;
  auto type = ROW({"r"}, {ROW({"r"}, {ROW({"a"}, {INTEGER()})})});

  auto leaf = makeFlatVector<int32_t>(kNumRows, [](auto row) { return row; });
  leaf->setNull(1, true);
  leaf->setNull(5, true);
  leaf->setNull(10, true);

  auto inner = makeRowVector(
      {"a"}, {leaf}, [](auto row) { return row == 3 || row == 6 || row == 8; });
  auto outer = makeRowVector({"r"}, {inner}, [](auto row) {
    return row == 1 || row == 6 || row == 11;
  });
  auto input = makeRowVector({"r"}, {outer});

  std::vector<uint32_t> partitions(kNumRows);
  for (int i = 0; i < kNumRows; ++i) {
    partitions[i] = i % 2;
  }

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, partitions);
  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  for (int parity = 0; parity < 2; ++parity) {
    auto page = deserialize(*ioBufs.at(parity).first, type);
    ASSERT_EQ(page->size(), kNumRows / 2) << "parity " << parity;

    auto* outerResult = page->childAt(0)->as<RowVector>();
    auto* innerResult = outerResult->childAt(0)->as<RowVector>();
    auto* leafResult = innerResult->childAt(0)->as<FlatVector<int32_t>>();

    int numOuterNulls{0};
    int numInnerNulls{0};
    int numLeafNulls{0};
    std::vector<int32_t> values;
    for (int row = 0; row < page->size(); ++row) {
      if (outerResult->isNullAt(row)) {
        ++numOuterNulls;
        continue;
      }
      if (innerResult->isNullAt(row)) {
        ++numInnerNulls;
        continue;
      }
      if (leafResult->isNullAt(row)) {
        ++numLeafNulls;
        continue;
      }
      values.push_back(leafResult->valueAt(row));
    }
    std::sort(values.begin(), values.end());

    int expectedOuterNulls{0};
    int expectedInnerNulls{0};
    int expectedLeafNulls{0};
    std::vector<int32_t> expectedValues;
    for (int row = parity; row < kNumRows; row += 2) {
      if (row == 1 || row == 6 || row == 11) {
        ++expectedOuterNulls;
        continue;
      }
      if (row == 3 || row == 8) {
        ++expectedInnerNulls;
        continue;
      }
      if (row == 5 || row == 10) {
        ++expectedLeafNulls;
        continue;
      }
      expectedValues.push_back(row);
    }

    EXPECT_EQ(numOuterNulls, expectedOuterNulls) << "parity " << parity;
    EXPECT_EQ(numInnerNulls, expectedInnerNulls) << "parity " << parity;
    EXPECT_EQ(numLeafNulls, expectedLeafNulls) << "parity " << parity;
    EXPECT_EQ(values, expectedValues) << "parity " << parity;
  }
}

// User example: page contains one column whose type is row(row(int)).
// The OUTER ROW column has nulls every 2nd row, the LEAF has nulls every
// 3rd row, and the inner ROW has no own nulls. Verifies:
//   * the OUTER column block emits a 10-row block with nulls at rows
//     0, 2, 4, 6, 8;
//   * the leaf's wire payload contains exactly the 3 non-null values
//     {1, 5, 7} that survive both ancestor filters;
//   * after deserialize, the leaf is null at outer-non-null positions
//     where the original leaf was null (rows 3 and 9).
TEST_F(PrestoIterativePartitioningSerializerNestedRowNullTest, userExample) {
  constexpr int kNumValues = 10;

  // Page schema: ROW({"col"}, {OUTER}). OUTER = row(row(int)).
  auto outerColType = ROW({"a"}, {ROW({"b"}, {INTEGER()})});
  auto pageType = ROW({"col"}, {outerColType});

  // Build leaf -> inner -> outer (with own nulls at rows 0,2,4,6,8) and
  // wrap once more so OUTER becomes a column of the page rather than the
  // page itself.
  auto leaf = makeFlatVector<int32_t>(
      kNumValues,
      [](auto row) { return static_cast<int32_t>(row); },
      nullEvery(3));
  auto inner = makeRowVector({"b"}, {leaf});
  auto outer = makeRowVector({"a"}, {inner}, nullEvery(2));
  auto pageInput = makeRowVector({"col"}, {outer});

  // Single partition keeps the wire format simple to reason about.
  std::vector<uint32_t> partitions(kNumValues, 0);

  auto serializer = makeSerializer(pageType, 1);
  serializer->append(pageInput, partitions);
  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 1);

  auto page = deserialize(*ioBufs.at(0).first, pageType);
  ASSERT_EQ(page->size(), kNumValues);

  // OUTER column block: 5 nulls at rows 0, 2, 4, 6, 8.
  auto* outerResult = page->childAt(0)->as<RowVector>();
  ASSERT_NE(outerResult, nullptr);
  for (int row = 0; row < kNumValues; ++row) {
    if (row % 2 == 0) {
      EXPECT_TRUE(outerResult->isNullAt(row)) << "row " << row;
    } else {
      EXPECT_FALSE(outerResult->isNullAt(row)) << "row " << row;
    }
  }

  // INNER ROW had no own nulls. Drill down to the leaf.
  auto* innerResult = outerResult->childAt(0)->as<RowVector>();
  ASSERT_NE(innerResult, nullptr);
  auto* leafResult = innerResult->childAt(0)->asFlatVector<int32_t>();
  ASSERT_NE(leafResult, nullptr);

  // Walk only outer-non-null rows. Leaf is null iff the original leaf was
  // null at that row.
  std::vector<int32_t> survivedValues;
  int leafNullsAtOuterLive{0};
  for (int row = 1; row < kNumValues; row += 2) {
    if (leafResult->isNullAt(row)) {
      ++leafNullsAtOuterLive;
    } else {
      survivedValues.push_back(leafResult->valueAt(row));
    }
  }
  EXPECT_EQ(leafNullsAtOuterLive, 2); // rows 3 and 9
  EXPECT_THAT(survivedValues, ::testing::ElementsAre(1, 5, 7));
}

TEST_F(
    PrestoIterativePartitioningSerializerNestedRowNullTest,
    parentNullsPropagateToNullFreeChildren) {
  constexpr int kNumRows = 8;
  auto type = ROW({"r"}, {ROW({"r"}, {ROW({"a"}, {INTEGER()})})});

  auto leaf = makeFlatVector<int32_t>(kNumRows, [](auto row) { return row; });
  auto inner = makeRowVector({"a"}, {leaf});
  auto outer = makeRowVector({"r"}, {inner}, [](auto row) {
    return row == 1 || row == 4 || row == 6;
  });
  auto input = makeRowVector({"r"}, {outer});

  std::vector<uint32_t> partitions(kNumRows);
  for (int i = 0; i < kNumRows; ++i) {
    partitions[i] = i % 2;
  }

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, partitions);
  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  for (int parity = 0; parity < 2; ++parity) {
    auto page = deserialize(*ioBufs.at(parity).first, type);
    ASSERT_EQ(page->size(), kNumRows / 2) << "parity " << parity;

    auto* outerResult = page->childAt(0)->as<RowVector>();
    auto* innerResult = outerResult->childAt(0)->as<RowVector>();
    auto* leafResult = innerResult->childAt(0)->as<FlatVector<int32_t>>();

    int numOuterNulls{0};
    std::vector<int32_t> values;
    for (int row = 0; row < page->size(); ++row) {
      if (outerResult->isNullAt(row)) {
        ++numOuterNulls;
        continue;
      }

      EXPECT_FALSE(leafResult->isNullAt(row))
          << "parity " << parity << " row " << row;
      values.push_back(leafResult->valueAt(row));
    }

    std::vector<int32_t> expectedValues;
    int expectedOuterNulls{0};
    for (int row = parity; row < kNumRows; row += 2) {
      if (row == 1 || row == 4 || row == 6) {
        ++expectedOuterNulls;
        continue;
      }
      expectedValues.push_back(row);
    }

    EXPECT_EQ(numOuterNulls, expectedOuterNulls) << "parity " << parity;
    EXPECT_EQ(values, expectedValues) << "parity " << parity;
  }
}

// Nulls at multiple nested ROW levels, spread over several appends and
// partitions. Exercises the bulk AND / countBits / extractBits compaction
// path in flushRowColumn: parent-null masks are combined down two ROW levels,
// rows are counted per partition, and the per-level null bitmap is compacted
// across batch boundaries before being flushed.
TEST_F(
    PrestoIterativePartitioningSerializerNestedRowNullTest,
    multiLevelNullsMultiAppendMultiPartition) {
  constexpr uint32_t kNumPartitions = 3;
  auto type = ROW({"r"}, {ROW({"r"}, {ROW({"a"}, {INTEGER()})})});

  // Two appends of different sizes. The outer ROW column is null on even
  // global indices; the inner ROW is null on multiples of 3 (only observable
  // where the outer ROW is non-null). The leaf carries the global index and
  // has no nulls of its own.
  struct Append {
    int size;
    int base;
  };
  const std::vector<Append> appends{{10, 0}, {7, 10}};

  auto serializer = makeSerializer(type, kNumPartitions);

  // Partitioning may reorder rows within a partition, so verify the per-row
  // outcomes as an order-independent multiset. Each entry is
  //   {0, 0}      -> outer ROW null,
  //   {1, 0}      -> inner ROW null (outer not null),
  //   {2, value}  -> leaf value present (no ancestor null).
  std::vector<std::vector<std::pair<int, int32_t>>> expected(kNumPartitions);

  for (const auto& a : appends) {
    auto leaf =
        makeFlatVector<int32_t>(a.size, [&](auto row) { return a.base + row; });
    auto inner = makeRowVector(
        {"a"}, {leaf}, [&](auto row) { return ((a.base + row) % 3) == 0; });
    auto outer = makeRowVector(
        {"r"}, {inner}, [&](auto row) { return ((a.base + row) % 2) == 0; });
    auto input = makeRowVector({"r"}, {outer});

    std::vector<uint32_t> partitions(a.size);
    for (int row = 0; row < a.size; ++row) {
      const int g = a.base + row;
      const uint32_t p = g % kNumPartitions;
      partitions[row] = p;
      if ((g % 2) == 0) {
        expected[p].emplace_back(0, 0);
      } else if ((g % 3) == 0) {
        expected[p].emplace_back(1, 0);
      } else {
        expected[p].emplace_back(2, g);
      }
    }
    serializer->append(input, partitions);
  }

  auto ioBufs = serializer->flush();

  for (uint32_t p = 0; p < kNumPartitions; ++p) {
    ASSERT_EQ(ioBufs.count(p), 1) << "partition " << p;
    auto page = deserialize(*ioBufs.at(p).first, type);
    ASSERT_EQ(page->size(), static_cast<int32_t>(expected[p].size()))
        << "partition " << p;

    auto* outerResult = page->childAt(0)->as<RowVector>();
    auto* innerResult = outerResult->childAt(0)->as<RowVector>();
    auto* leafResult = innerResult->childAt(0)->as<FlatVector<int32_t>>();

    std::vector<std::pair<int, int32_t>> actual;
    actual.reserve(page->size());
    for (int row = 0; row < page->size(); ++row) {
      if (outerResult->isNullAt(row)) {
        actual.emplace_back(0, 0);
      } else if (innerResult->isNullAt(row)) {
        actual.emplace_back(1, 0);
      } else {
        ASSERT_FALSE(leafResult->isNullAt(row))
            << "partition " << p << " row " << row;
        actual.emplace_back(2, leafResult->valueAt(row));
      }
    }

    std::sort(actual.begin(), actual.end());
    std::sort(expected[p].begin(), expected[p].end());
    EXPECT_EQ(actual, expected[p]) << "partition " << p;
  }
}

// flush() must not modify the vectors handed to append(): the caller may still
// be holding them. Combining an ancestor's null mask into a level's own nulls
// therefore needs a buffer of its own rather than the input's null bitmap.
TEST_F(
    PrestoIterativePartitioningSerializerNestedRowNullTest,
    flushDoesNotModifyInput) {
  auto type = ROW({"r"}, {ROW({"r"}, {ROW({"a"}, {INTEGER()})})});

  // The inner ROW is null at row 2 only, the outer ROW at row 0 only, so the
  // two masks must be combined for the leaf.
  auto leaf = makeFlatVector<int32_t>({0, 1, 2, 3});
  auto inner = makeRowVector({"a"}, {leaf}, [](auto row) { return row == 2; });
  auto outer = makeRowVector({"r"}, {inner}, [](auto row) { return row == 0; });

  const auto* innerNullsBefore = inner->rawNulls();
  ASSERT_EQ(BaseVector::countNulls(inner->nulls(), 4), 1);

  auto serializer = makeSerializer(type, 1);
  serializer->append(makeRowVector({"r"}, {outer}), 0u);
  auto ioBufs = serializer->flush();

  EXPECT_EQ(inner->rawNulls(), innerNullsBefore);
  EXPECT_EQ(BaseVector::countNulls(inner->nulls(), 4), 1);
  EXPECT_FALSE(inner->isNullAt(0));

  // The combined mask must still be applied to what is written.
  ASSERT_EQ(ioBufs.size(), 1);
  auto page = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(page->size(), 4);
  auto* outerResult = page->childAt(0)->as<RowVector>();
  auto* innerResult = outerResult->childAt(0)->as<RowVector>();
  auto* leafResult = innerResult->childAt(0)->as<FlatVector<int32_t>>();
  EXPECT_TRUE(outerResult->isNullAt(0));
  EXPECT_TRUE(innerResult->isNullAt(2));
  EXPECT_EQ(leafResult->valueAt(1), 1);
  EXPECT_EQ(leafResult->valueAt(3), 3);
}

// A VARCHAR leaf under a ROW round-trips as long as no ancestor ROW drops
// rows. A null bitmap that is merely materialized, with no null set, drops
// nothing and must not be mistaken for one that does.
TEST_F(PrestoIterativePartitioningSerializerNestedRowNullTest, nestedVarchar) {
  auto type = ROW({"r"}, {ROW({"s"}, {VARCHAR()})});

  auto inner = makeRowVector(
      {"s"}, {makeFlatVector<std::string>({"aa", "bb", "cc", "dd"})});
  // Materialize an all-not-null bitmap the way setNull()/ensureWritable() do.
  inner->setNull(0, true);
  inner->setNull(0, false);
  ASSERT_NE(inner->rawNulls(), nullptr);
  ASSERT_EQ(BaseVector::countNulls(inner->nulls(), 4), 0);

  auto serializer = makeSerializer(type, 2);
  serializer->append(makeRowVector({"r"}, {inner}), {0, 1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto leafValues = [&](uint32_t partition) {
    auto page = deserialize(*ioBufs.at(partition).first, type);
    auto* leaf = page->childAt(0)
                     ->as<RowVector>()
                     ->childAt(0)
                     ->as<FlatVector<StringView>>();
    std::vector<std::string> values;
    for (auto row = 0; row < page->size(); ++row) {
      values.push_back(leaf->valueAt(row).str());
    }
    return values;
  };

  EXPECT_THAT(leafValues(0), testing::ElementsAre("aa", "cc"));
  EXPECT_THAT(leafValues(1), testing::ElementsAre("bb", "dd"));
}

// A VARCHAR leaf whose ancestor ROW actually drops rows is not supported yet.
// The buffered offsets and values cover every partitioned row, so the writer
// cannot skip the rows the ancestor discards.
TEST_F(
    PrestoIterativePartitioningSerializerNestedRowNullTest,
    nestedVarcharUnderNullRow) {
  auto type = ROW({"r"}, {ROW({"s"}, {VARCHAR()})});

  auto inner = makeRowVector(
      {"s"},
      {makeFlatVector<std::string>({"aa", "bb", "cc", "dd"})},
      [](auto row) { return row == 0; });

  auto serializer = makeSerializer(type, 2);
  serializer->append(makeRowVector({"r"}, {inner}), {0, 1, 0, 1});

  VELOX_ASSERT_THROW(
      serializer->flush(),
      "Variable-width columns nested under a ROW with nulls are not supported");
}

// A struct column can arrive CONSTANT-encoded, in which case
// PartitionedVector::create() produces a PartitionedConstantVector rather than
// a PartitionedRowVector. Both the size estimate and append() must report the
// unsupported encoding instead of misinterpreting the vector.
TEST_F(
    PrestoIterativePartitioningSerializerNestedRowNullTest,
    constantEncodedRowColumn) {
  auto type = ROW({"r"}, {ROW({"a"}, {INTEGER()})});
  auto serializer = makeSerializer(type, 2);

  auto input = makeRowVector(
      {"r"},
      {BaseVector::wrapInConstant(
          4, 0, makeRowVector({"a"}, {makeFlatVector<int32_t>({7})}))});

  VELOX_ASSERT_THROW(
      serializer->estimateBytesAfterAppend(input),
      "Unsupported encoding for a ROW column");
  VELOX_ASSERT_THROW(
      serializer->append(input, {0, 1, 0, 1}),
      "Unsupported encoding for a ROW column");
}

INSTANTIATE_TEST_SUITE_P(
    Levels,
    PrestoIterativePartitioningSerializerNestedRowTest,
    ::testing::Values(1, 2, 3),
    [](const ::testing::TestParamInfo<int>& info) {
      return "level" + std::to_string(info.param);
    });

// ---------------------------------------------------------------------------
// Non-typed fixture (TEST_F) — lifecycle, structural, regression
// ---------------------------------------------------------------------------

class PrestoIterativePartitioningSerializerTest
    : public ::testing::Test,
      public PrestoIterativePartitioningSerializerTestBase {
 public:
  static void SetUpTestSuite() {
    PrestoIterativePartitioningSerializerTestBase::SetUpTestSuite();
  }
};

// Appending an empty RowVector produces no ioBufs on flush.
TEST_F(PrestoIterativePartitioningSerializerTest, appendEmptyVector) {
  auto type = ROW({"a"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);
  serializer->append(makeRowVector({"a"}, {makeFlatVector<int64_t>({})}), {});
  EXPECT_TRUE(serializer->flush().empty());
}

// ── Lifecycle
// ─────────────────────────────────────────────────────────────────

// Multiple append() calls accumulate correctly before flush.
TEST_F(PrestoIterativePartitioningSerializerTest, multipleAppends) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 3);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({100, 200, 300})}),
      {0, 1, 2});
  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({400, 500, 600})}),
      {2, 0, 1});

  EXPECT_EQ(serializer->rowsBuffered(), 6);
  const auto bufferedBytes = serializer->bytesBuffered();

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 3);
  EXPECT_EQ(bufferedBytes, totalFlushedBytes(ioBufs));

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  auto r1 = deserialize(*ioBufs.at(1).first, type);
  auto r2 = deserialize(*ioBufs.at(2).first, type);

  ASSERT_EQ(r0->size(), 2);
  ASSERT_EQ(r1->size(), 2);
  ASSERT_EQ(r2->size(), 2);

  EXPECT_EQ(sortedValues<int64_t>(r0, 0), (std::vector<int64_t>{100, 500}));
  EXPECT_EQ(sortedValues<int64_t>(r1, 0), (std::vector<int64_t>{200, 600}));
  EXPECT_EQ(sortedValues<int64_t>(r2, 0), (std::vector<int64_t>{300, 400}));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    dictionaryScalarInputRoundTrip) {
  auto type = ROW({"v"}, {BIGINT()});

  auto base = makeNullableFlatVector<int64_t>(
      {10, std::nullopt, 30, 40, 50, std::nullopt});
  auto dictionary = BaseVector::wrapInDictionary(
      makeNulls({false, true, false, false, false, false}),
      makeIndices({5, 4, 3, 2, 1, 0}),
      6,
      base);
  auto input = makeRowVector({"v"}, {dictionary});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 1, 0, 1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto p0 = deserialize(*ioBufs.at(0).first, type);
  auto p1 = deserialize(*ioBufs.at(1).first, type);

  auto expectedP0 = makeRowVector(
      {"v"},
      {makeNullableFlatVector<int64_t>({std::nullopt, 40, std::nullopt})});
  auto expectedP1 = makeRowVector(
      {"v"}, {makeNullableFlatVector<int64_t>({std::nullopt, 30, 10})});

  assertEqualVectors(canonicalize(expectedP0), canonicalize(p0));
  assertEqualVectors(canonicalize(expectedP1), canonicalize(p1));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    dictionaryScalarSinglePartitionFastPathRoundTrip) {
  auto type = ROW({"v"}, {BIGINT()});

  auto base = makeNullableFlatVector<int64_t>({10, std::nullopt, 30});
  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, makeIndices({2, 1, 0}), 3, base);
  auto input = makeRowVector({"v"}, {dictionary});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, /*singlePartition=*/1);

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 1);
  ASSERT_TRUE(ioBufs.count(1));

  auto actual = deserialize(*ioBufs.at(1).first, type);
  auto expected = makeRowVector(
      {"v"}, {makeNullableFlatVector<int64_t>({30, std::nullopt, 10})});
  assertEqualVectors(expected, actual);
}

TEST_F(PrestoIterativePartitioningSerializerTest, dictionaryBooleanRoundTrip) {
  auto type = ROW({"v"}, {BOOLEAN()});
  auto serializer = makeSerializer(type, 2);

  // First append has wrapper nulls and base nulls, exercising the
  // null-skipping write path.
  auto nullableBase = makeNullableFlatVector<bool>({true, std::nullopt, false});
  auto nullableDictionary = BaseVector::wrapInDictionary(
      makeNulls({true, false, false, false, false, false}),
      makeIndices({2, 1, 0, 2, 1, 0}),
      6,
      nullableBase);
  serializer->append(
      makeRowVector({"v"}, {nullableDictionary}), {0, 1, 0, 1, 0, 1});

  // Second append is null-free, exercising the chunked fast path.
  auto nullFreeDictionary = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices({1, 0, 1, 0, 1, 0}),
      6,
      makeFlatVector<bool>({false, true}));
  serializer->append(
      makeRowVector({"v"}, {nullFreeDictionary}), {0, 1, 0, 1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto p0 = deserialize(*ioBufs.at(0).first, type);
  auto p1 = deserialize(*ioBufs.at(1).first, type);

  auto expectedP0 = makeRowVector(
      {"v"},
      {makeNullableFlatVector<bool>(
          {std::nullopt, true, std::nullopt, true, true, true})});
  auto expectedP1 = makeRowVector(
      {"v"},
      {makeNullableFlatVector<bool>(
          {std::nullopt, false, true, false, false, false})});

  assertEqualVectors(canonicalize(expectedP0), canonicalize(p0));
  assertEqualVectors(canonicalize(expectedP1), canonicalize(p1));
}

// Dictionary partitions larger than one write chunk exercise the chunked
// flush loops, both null-free and with wrapper nulls.
TEST_F(PrestoIterativePartitioningSerializerTest, dictionaryChunkedFlush) {
  constexpr vector_size_t kNumRows = 10'000;
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);

  std::vector<uint32_t> partitions(kNumRows);
  for (vector_size_t row = 0; row < kNumRows; ++row) {
    partitions[row] = row % 2;
  }

  auto base = makeFlatVector<int64_t>({100, 200, 300});
  auto nullFree = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(kNumRows, [](auto row) { return row % 3; }),
      kNumRows,
      base);
  serializer->append(makeRowVector({"v"}, {nullFree}), partitions);

  auto nullable = BaseVector::wrapInDictionary(
      makeNulls(kNumRows, [](auto row) { return row % 7 == 0; }),
      makeIndices(kNumRows, [](auto row) { return (row + 1) % 3; }),
      kNumRows,
      base);
  serializer->append(makeRowVector({"v"}, {nullable}), partitions);

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  for (uint32_t p = 0; p < 2; ++p) {
    std::vector<std::optional<int64_t>> expectedValues;
    for (vector_size_t row = p; row < kNumRows; row += 2) {
      expectedValues.push_back(100 * (row % 3 + 1));
    }
    for (vector_size_t row = p; row < kNumRows; row += 2) {
      if (row % 7 == 0) {
        expectedValues.push_back(std::nullopt);
      } else {
        expectedValues.push_back(100 * ((row + 1) % 3 + 1));
      }
    }

    auto actual = deserialize(*ioBufs.at(p).first, type);
    auto expected =
        makeRowVector({"v"}, {makeNullableFlatVector<int64_t>(expectedValues)});
    assertEqualVectors(canonicalize(expected), canonicalize(actual));
  }
}

TEST_F(PrestoIterativePartitioningSerializerTest, dictionaryRowInputRoundTrip) {
  auto nestedType = ROW({"a", "b"}, {INTEGER(), BIGINT()});
  auto type = ROW({"r"}, {nestedType});

  auto baseRow = makeRowVector(
      {"a", "b"},
      {makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt}),
       makeFlatVector<int64_t>({10, 20, 30, 40})});
  auto dictionaryRow = BaseVector::wrapInDictionary(
      makeNulls({false, true, false, false, false}),
      makeIndices({3, 2, 1, 0, 2}),
      5,
      baseRow);
  auto input = makeRowVector({"r"}, {dictionaryRow});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 1, 0, 1, 0});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto p0 = deserialize(*ioBufs.at(0).first, type);
  auto p1 = deserialize(*ioBufs.at(1).first, type);

  auto expectedP0 = makeRowVector(
      {"r"},
      {makeRowVector(
          {"a", "b"},
          {makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
           makeFlatVector<int64_t>({40, 20, 30})})});
  auto expectedP1 = makeRowVector(
      {"r"},
      {makeRowVector(
          {"a", "b"},
          {makeFlatVector<int32_t>({0, 1}), makeFlatVector<int64_t>({0, 10})},
          [](vector_size_t row) { return row == 0; })});

  assertEqualVectors(canonicalize(expectedP0), canonicalize(p0));
  assertEqualVectors(canonicalize(expectedP1), canonicalize(p1));
}

// Dictionary children under a ROW column with row-level nulls: the values of
// rows whose parent is null must be omitted from the wire, for dictionaries
// both with and without their own nulls.
TEST_F(
    PrestoIterativePartitioningSerializerTest,
    dictionaryUnderNullableRowRoundTrip) {
  auto nestedType = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  auto type = ROW({"r"}, {nestedType});

  auto nullableChild = BaseVector::wrapInDictionary(
      makeNulls({true, false, false, true, false, false}),
      makeIndices({2, 1, 0, 2, 1, 0}),
      6,
      makeFlatVector<int64_t>({100, 200, 300}));
  auto nullFreeChild = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices({0, 1, 2, 0, 1, 2}),
      6,
      makeFlatVector<int64_t>({7, 8, 9}));
  auto nestedRow = makeRowVector(
      {"a", "b"}, {nullableChild, nullFreeChild}, [](vector_size_t row) {
        return row == 1 || row == 4;
      });
  auto input = makeRowVector({"r"}, {nestedRow});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 1, 0, 1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto p0 = deserialize(*ioBufs.at(0).first, type);
  auto p1 = deserialize(*ioBufs.at(1).first, type);

  // Partition 0 holds input rows 0, 2, 4; row 4 has a null parent. Partition
  // 1 holds input rows 1, 3, 5; row 1 has a null parent. Children of a null
  // parent deserialize to default values.
  auto expectedP0 = makeRowVector(
      {"r"},
      {makeRowVector(
          {"a", "b"},
          {makeNullableFlatVector<int64_t>({std::nullopt, 100, 0}),
           makeFlatVector<int64_t>({7, 9, 0})},
          [](vector_size_t row) { return row == 2; })});
  auto expectedP1 = makeRowVector(
      {"r"},
      {makeRowVector(
          {"a", "b"},
          {makeNullableFlatVector<int64_t>({0, std::nullopt, 100}),
           makeFlatVector<int64_t>({0, 7, 9})},
          [](vector_size_t row) { return row == 0; })});

  assertEqualVectors(canonicalize(expectedP0), canonicalize(p0));
  assertEqualVectors(canonicalize(expectedP1), canonicalize(p1));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    bytesBufferedPartitionGrowth) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);

  const auto singleRowPageBytes = simpleColumnPageBytes("LONG_ARRAY", 1, 0, 8);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({10})}),
      std::vector<uint32_t>{0});
  EXPECT_EQ(serializer->bytesBuffered(), singleRowPageBytes);

  auto input = makeRowVector({"v"}, {makeFlatVector<int64_t>({20})});
  EXPECT_EQ(serializer->bytesBuffered(), singleRowPageBytes);

  serializer->append(input, std::vector<uint32_t>{1});
  const auto bytesBuffered = serializer->bytesBuffered();
  EXPECT_EQ(serializer->bytesBuffered(), 2 * singleRowPageBytes);

  auto ioBufs = serializer->flush();
  EXPECT_EQ(serializer->bytesBuffered(), 0);
  EXPECT_EQ(bytesBuffered, totalFlushedBytes(ioBufs));
}

TEST_F(PrestoIterativePartitioningSerializerTest, bytesBufferedNullFlagGrowth) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 1);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8})}),
      std::vector<uint32_t>(8, 0));
  EXPECT_EQ(
      serializer->bytesBuffered(),
      simpleColumnPageBytes("LONG_ARRAY", 8, 0, 8));

  auto input =
      makeRowVector({"v"}, {makeNullableFlatVector<int64_t>({std::nullopt})});
  EXPECT_EQ(
      serializer->bytesBuffered(),
      simpleColumnPageBytes("LONG_ARRAY", 8, 0, 8));

  serializer->append(input, std::vector<uint32_t>{0});
  const auto bytesBuffered = serializer->bytesBuffered();
  EXPECT_EQ(bytesBuffered, simpleColumnPageBytes("LONG_ARRAY", 9, 1, 8));

  auto ioBufs = serializer->flush();
  EXPECT_EQ(serializer->bytesBuffered(), 0);
  EXPECT_EQ(bytesBuffered, totalFlushedBytes(ioBufs));
}

// A flush time output mapping serializes one input colum into multiple output
// columns.
TEST_F(
    PrestoIterativePartitioningSerializerTest,
    duplicateOutputColumnAtFlush) {
  auto outputType = ROW({"v1", "v2"}, {BIGINT(), BIGINT()});
  SerdeOpts opts;
  auto serializer = std::make_unique<PrestoIterativePartitioningSerializer>(
      outputType, 2, opts, pool_.get(), std::vector<column_index_t>{0, 0});

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({10, 11, 12, 13})}),
      {0, 1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, outputType);
  auto r1 = deserialize(*ioBufs.at(1).first, outputType);

  ASSERT_EQ(r0->size(), 2);
  ASSERT_EQ(r1->size(), 2);

  EXPECT_EQ(sortedValues<int64_t>(r0, 0), (std::vector<int64_t>{10, 12}));
  EXPECT_EQ(sortedValues<int64_t>(r0, 1), (std::vector<int64_t>{10, 12}));
  EXPECT_EQ(sortedValues<int64_t>(r1, 0), (std::vector<int64_t>{11, 13}));
  EXPECT_EQ(sortedValues<int64_t>(r1, 1), (std::vector<int64_t>{11, 13}));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    outputInputMappingOutOfRange) {
  auto outputType = ROW({"v1", "v2"}, {BIGINT(), BIGINT()});
  SerdeOpts opts;
  auto serializer = std::make_unique<PrestoIterativePartitioningSerializer>(
      outputType, 2, opts, pool_.get(), std::vector<column_index_t>{0, 1});

  VELOX_ASSERT_THROW(
      serializer->append(
          makeRowVector({"v"}, {makeFlatVector<int64_t>({10, 11})}), {0, 1}),
      "Output column 1 maps to invalid input column 1");
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    outputInputMappingTypeMismatch) {
  auto outputType = ROW({"v1", "v2"}, {BIGINT(), BIGINT()});
  SerdeOpts opts;
  auto serializer = std::make_unique<PrestoIterativePartitioningSerializer>(
      outputType, 2, opts, pool_.get(), std::vector<column_index_t>{0, 1});

  VELOX_ASSERT_THROW(
      serializer->append(
          makeRowVector(
              {"v1", "v2"},
              {
                  makeFlatVector<int64_t>({10, 11}),
                  makeFlatVector<int32_t>({12, 13}),
              }),
          {0, 1}),
      "Output column 1 expects BIGINT, got INTEGER from input column 1");
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendExactForSinglePartition) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 1);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8})}),
      std::vector<uint32_t>(8, 0));

  auto input =
      makeRowVector({"v"}, {makeNullableFlatVector<int64_t>({std::nullopt})});
  const auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  serializer->append(input, std::vector<uint32_t>{0});
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendExactForConstant) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 1);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3, 4})}),
      std::vector<uint32_t>(4, 0));

  auto input = makeRowVector({"v"}, {makeConstant<int64_t>(7, 2)});
  const auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  serializer->append(input, std::vector<uint32_t>(2, 0));
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendExactForNullConstant) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 1);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8})}),
      std::vector<uint32_t>(8, 0));

  auto input = makeRowVector({"v"}, {makeConstant<int64_t>(std::nullopt, 80)});
  const auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  serializer->append(input, std::vector<uint32_t>(80, 0));
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendExactForSinglePartitionVarchar) {
  auto type = ROW({"v"}, {VARCHAR()});
  auto serializer = makeSerializer(type, 1);

  const std::string longValue(StringView::kInlineSize + 6, 'a');
  serializer->append(
      makeRowVector(
          {"v"},
          {makeFlatVector<std::string>({"short", longValue, "", "tail"})}),
      std::vector<uint32_t>(4, 0));

  auto input = makeRowVector(
      {"v"},
      {makeNullableFlatVector<std::string>(
          {std::nullopt,
           "next",
           std::string(StringView::kInlineSize + 8, 'b')})});
  const auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  serializer->append(input, std::vector<uint32_t>(3, 0));
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendExactForDictionary) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 3);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3, 4})}),
      std::vector<uint32_t>(4, 0));

  auto base = makeNullableFlatVector<int64_t>({10, std::nullopt, 30});
  auto dictionary = BaseVector::wrapInDictionary(
      makeNulls({false, false, true}), makeIndices({2, 1, 0}), 3, base);
  auto input = makeRowVector({"v"}, {dictionary});

  auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  // input lands on new partitions
  serializer->append(input, {1, 1, 2});
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());

  estimatedAfter = serializer->estimateBytesAfterAppend(input);

  // input lands on existing partitions
  serializer->append(input, {0, 2, 0});
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendExactForDictionaryRow) {
  auto nestedType = ROW({"a"}, {INTEGER()});
  auto type = ROW({"r"}, {nestedType});
  auto serializer = makeSerializer(type, 3);

  serializer->append(
      makeRowVector(
          {"r"},
          {makeRowVector(
              {"a"},
              {makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4})})}),
      std::vector<uint32_t>(4, 0));

  auto baseRow = makeRowVector(
      {"a"}, {makeNullableFlatVector<int32_t>({10, std::nullopt, 30})});
  auto dictionaryRow =
      BaseVector::wrapInDictionary(nullptr, makeIndices({2, 1, 0}), 3, baseRow);
  auto input = makeRowVector({"r"}, {dictionaryRow});

  auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  // input lands on new partitions
  serializer->append(input, {1, 1, 2});
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());

  estimatedAfter = serializer->estimateBytesAfterAppend(input);

  // input lands on existing partitions
  serializer->append(input, {0, 2, 0});
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendExactForSinglePartitionDictionaryVarchar) {
  auto type = ROW({"v"}, {VARCHAR()});
  auto serializer = makeSerializer(type, 1);

  const std::string seedLong(StringView::kInlineSize + 3, 's');
  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<std::string>({"seed", seedLong})}),
      std::vector<uint32_t>(2, 0));

  const std::string long1(StringView::kInlineSize + 5, 'x');
  const std::string long2 =
      std::string(StringView::kInlineSize + 8, 'y') + "-tail";
  auto base = makeNullableFlatVector<std::string>(
      {long1, std::nullopt, "", "tail", long2});
  auto dictionary = BaseVector::wrapInDictionary(
      makeNulls({false, true, false, false, false, false}),
      makeIndices({4, 2, 1, 0, 3, 2}),
      6,
      base);
  auto input = makeRowVector({"v"}, {dictionary});

  const auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  serializer->append(input, std::vector<uint32_t>(6, 0));
  EXPECT_EQ(estimatedAfter, serializer->bytesBuffered());
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    estimateBytesAfterAppendOverestimatesPartitionedAppend) {
  auto type = ROW({"a", "b"}, {BIGINT(), INTEGER()});
  auto serializer = makeSerializer(type, 3);

  serializer->append(
      makeRowVector(
          {"a", "b"},
          {
              makeFlatVector<int64_t>({10, 20}),
              makeFlatVector<int32_t>({100, 200}),
          }),
      {0, 1});

  auto input = makeRowVector(
      {"a", "b"},
      {
          makeNullableFlatVector<int64_t>({30, std::nullopt, 50, 60}),
          makeNullableFlatVector<int32_t>({300, 400, std::nullopt, 600}),
      });

  // All rows land in an already non-empty partition, but
  // estimateBytesAfterAppend still assume this input could go to the last empty
  // partition before the real distribution is known.
  const std::vector<uint32_t> partitions{1, 1, 1, 1};

  const auto estimatedAfter = serializer->estimateBytesAfterAppend(input);

  serializer->append(input, partitions);
  EXPECT_GT(estimatedAfter, serializer->bytesBuffered());
}

// Flush twice: second flush on empty state returns an empty map.
TEST_F(PrestoIterativePartitioningSerializerTest, flushTwice) {
  auto type = ROW({"a"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);
  serializer->append(
      makeRowVector({"a"}, {makeFlatVector<int64_t>({10, 20})}), {0, 1});

  auto ioBufs1 = serializer->flush();
  ASSERT_EQ(ioBufs1.size(), 2);

  EXPECT_TRUE(serializer->flush().empty());
}

// Append and flush multiple independent cycles.
TEST_F(PrestoIterativePartitioningSerializerTest, multipleCycles) {
  auto type = ROW({"a"}, {INTEGER()});
  auto serializer = makeSerializer(type, 2);

  for (int cycle = 0; cycle < 3; ++cycle) {
    serializer->append(
        makeRowVector(
            {"a"}, {makeFlatVector<int32_t>({cycle * 2, cycle * 2 + 1})}),
        {0, 1});
    auto ioBufs = serializer->flush();
    ASSERT_EQ(ioBufs.size(), 2) << "cycle " << cycle;

    auto r0 = deserialize(*ioBufs.at(0).first, type);
    auto r1 = deserialize(*ioBufs.at(1).first, type);
    ASSERT_EQ(r0->size(), 1) << "cycle " << cycle;
    ASSERT_EQ(r1->size(), 1) << "cycle " << cycle;
    EXPECT_EQ(r0->childAt(0)->as<FlatVector<int32_t>>()->valueAt(0), cycle * 2);
    EXPECT_EQ(
        r1->childAt(0)->as<FlatVector<int32_t>>()->valueAt(0), cycle * 2 + 1);
  }
}

// ── Encoding
// ─────────────────────────────────────────────────────────────────

// Constant vectors are flattened across append() calls.
TEST_F(PrestoIterativePartitioningSerializerTest, constantColumnAcrossAppends) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 3);

  serializer->append(
      makeRowVector({"v"}, {makeConstant<int64_t>(11, 4)}), {0, 1, 0, 2});
  serializer->append(
      makeRowVector({"v"}, {makeConstant<int64_t>(22, 5)}), {2, 0, 1, 1, 2});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 3);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  auto r1 = deserialize(*ioBufs.at(1).first, type);
  auto r2 = deserialize(*ioBufs.at(2).first, type);

  EXPECT_EQ(sortedValues<int64_t>(r0, 0), (std::vector<int64_t>{11, 11, 22}));
  EXPECT_EQ(sortedValues<int64_t>(r1, 0), (std::vector<int64_t>{11, 22, 22}));
  EXPECT_EQ(sortedValues<int64_t>(r2, 0), (std::vector<int64_t>{11, 22, 22}));
}

// Boolean constant vectors are flattened across append() calls.
TEST_F(
    PrestoIterativePartitioningSerializerTest,
    booleanConstantColumnAcrossAppends) {
  auto type = ROW({"v"}, {BOOLEAN()});
  auto serializer = makeSerializer(type, 2);

  serializer->append(
      makeRowVector({"v"}, {makeConstant<bool>(true, 4)}), {0, 1, 0, 1});
  serializer->append(
      makeRowVector({"v"}, {makeConstant<bool>(false, 3)}), {1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  auto r1 = deserialize(*ioBufs.at(1).first, type);

  EXPECT_EQ(sortedValues<bool>(r0, 0), (std::vector<bool>{false, true, true}));
  EXPECT_EQ(
      sortedValues<bool>(r1, 0), (std::vector<bool>{false, false, true, true}));
}

// Boolean partitions larger than one 4096-row write chunk exercise the
// chunked flush loops, both null-free and with nulls.
TEST_F(PrestoIterativePartitioningSerializerTest, booleanChunkedFlush) {
  constexpr vector_size_t kNumRows = 10'000;
  auto type = ROW({"v"}, {BOOLEAN()});
  auto serializer = makeSerializer(type, 2);

  std::vector<uint32_t> partitions(kNumRows);
  for (vector_size_t row = 0; row < kNumRows; ++row) {
    partitions[row] = row % 2;
  }

  auto nullFree =
      makeFlatVector<bool>(kNumRows, [](auto row) { return row % 3 == 0; });
  serializer->append(makeRowVector({"v"}, {nullFree}), partitions);

  auto nullable = makeFlatVector<bool>(
      kNumRows, [](auto row) { return row % 2 == 0; }, nullEvery(7));
  serializer->append(makeRowVector({"v"}, {nullable}), partitions);

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  for (uint32_t p = 0; p < 2; ++p) {
    std::vector<std::optional<bool>> expectedValues;
    for (vector_size_t row = p; row < kNumRows; row += 2) {
      expectedValues.push_back(row % 3 == 0);
    }
    for (vector_size_t row = p; row < kNumRows; row += 2) {
      if (row % 7 == 0) {
        expectedValues.push_back(std::nullopt);
      } else {
        expectedValues.push_back(row % 2 == 0);
      }
    }

    auto actual = deserialize(*ioBufs.at(p).first, type);
    auto expected =
        makeRowVector({"v"}, {makeNullableFlatVector<bool>(expectedValues)});
    assertEqualVectors(canonicalize(expected), canonicalize(actual));
  }
}

// Null constant vectors contribute only nulls but still advance row positions.
TEST_F(
    PrestoIterativePartitioningSerializerTest,
    nullConstantColumnAcrossAppends) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);

  serializer->append(
      makeRowVector({"v"}, {makeConstant<int64_t>(std::nullopt, 3)}),
      {0, 1, 0});
  serializer->append(
      makeRowVector({"v"}, {makeConstant<int64_t>(7, 3)}), {1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  auto r1 = deserialize(*ioBufs.at(1).first, type);

  auto actual0 = nullableValues<int64_t>(r0, 0);
  std::sort(actual0.begin(), actual0.end());
  auto expected0 =
      std::vector<std::optional<int64_t>>{std::nullopt, std::nullopt, 7};
  EXPECT_EQ(actual0, expected0);

  auto actual1 = nullableValues<int64_t>(r1, 0);
  std::sort(actual1.begin(), actual1.end());
  auto expected1 = std::vector<std::optional<int64_t>>{std::nullopt, 7, 7};
  EXPECT_EQ(actual1, expected1);
}

// Constant and flat vectors are flattened and serialized correctly across
// append() calls.
TEST_F(PrestoIterativePartitioningSerializerTest, mixedConstantFlatVector) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);

  serializer->append(
      makeRowVector({"v"}, {makeConstant<int64_t>(7, 3)}), {0, 1, 0});
  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3})}), {1, 1, 0});
  serializer->append(
      makeRowVector({"v"}, {makeConstant<int64_t>(8, 2)}), {0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  auto r1 = deserialize(*ioBufs.at(1).first, type);

  EXPECT_EQ(sortedValues<int64_t>(r0, 0), (std::vector<int64_t>{3, 7, 7, 8}));
  EXPECT_EQ(sortedValues<int64_t>(r1, 0), (std::vector<int64_t>{1, 2, 7, 8}));
}

// Null constant rows are preserved and serialized correctly with flat and
// nullable flat vectors across append() calls.
TEST_F(PrestoIterativePartitioningSerializerTest, mixedNullConstantFlatVector) {
  auto type = ROW({"v"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);

  serializer->append(
      makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3, 4})}),
      {0, 1, 1, 0});
  serializer->append(
      makeRowVector({"v"}, {makeConstant<int64_t>(std::nullopt, 3)}),
      {0, 1, 0});
  serializer->append(
      makeRowVector(
          {"v"}, {makeNullableFlatVector<int64_t>({std::nullopt, 7, 3})}),
      {1, 0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  auto r1 = deserialize(*ioBufs.at(1).first, type);

  auto actual0 = nullableValues<int64_t>(r0, 0);
  std::sort(actual0.begin(), actual0.end());
  auto expected0 =
      std::vector<std::optional<int64_t>>{std::nullopt, std::nullopt, 1, 4, 7};
  EXPECT_EQ(actual0, expected0);

  auto actual1 = nullableValues<int64_t>(r1, 0);
  std::sort(actual1.begin(), actual1.end());
  auto expected1 =
      std::vector<std::optional<int64_t>>{std::nullopt, std::nullopt, 2, 3, 3};
  EXPECT_EQ(actual1, expected1);
}

TEST_F(PrestoIterativePartitioningSerializerTest, varcharWithoutNulls) {
  auto type = ROW({"s"}, {VARCHAR()});

  const std::string long1(StringView::kInlineSize + 5, 'a');
  const std::string long2 =
      std::string(StringView::kInlineSize + 9, 'b') + "-suffix";

  auto serializer = makeSerializer(type, 2);
  serializer->append(
      makeRowVector(
          {"s"},
          {makeFlatVector<std::string>({"short", long1, "", "tail", long2})}),
      {1, 0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 2);
  EXPECT_EQ(
      sortedValues<StringView>(r0, 0),
      (std::vector<StringView>{"", StringView(long1)}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 3);
  EXPECT_EQ(
      sortedValues<StringView>(r1, 0),
      (std::vector<StringView>{
          StringView(long2), StringView("short"), StringView("tail")}));
}

TEST_F(PrestoIterativePartitioningSerializerTest, dictionaryVarcharRoundTrip) {
  auto type = ROW({"s"}, {VARCHAR()});

  const std::string long1(StringView::kInlineSize + 5, 'a');
  const std::string long2 =
      std::string(StringView::kInlineSize + 9, 'b') + "-suffix";
  auto base = makeNullableFlatVector<std::string>(
      {long1, std::nullopt, "", "tail", long2});
  auto dictionary = BaseVector::wrapInDictionary(
      makeNulls({false, true, false, false, false, false}),
      makeIndices({4, 2, 1, 0, 3, 2}),
      6,
      base);

  auto serializer = makeSerializer(type, 2);
  serializer->append(makeRowVector({"s"}, {dictionary}), {0, 1, 0, 1, 0, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 3);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r0, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, StringView(long2), "tail"}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 3);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r1, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, "", StringView(long1)}));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    varcharWithNullsMultipleAppend) {
  auto type = ROW({"s"}, {VARCHAR()});

  const std::string long1(StringView::kInlineSize + 4, 'x');
  const std::string long2 =
      std::string(StringView::kInlineSize + 7, 'x') + "-suffix";
  const std::string long3 =
      std::string(StringView::kInlineSize + 10, 'x') + "-tail";

  auto serializer = makeSerializer(type, 2);
  serializer->append(
      makeRowVector(
          {"s"},
          {makeNullableFlatVector<std::string>(
              {"short", long1, std::nullopt, "", long2})}),
      {0, 1, 0, 1, 0});
  serializer->append(
      makeRowVector(
          {"s"},
          {makeNullableFlatVector<std::string>({"tail", std::nullopt, long3})}),
      {0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 4);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r0, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, "short", "tail", StringView(long2)}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 4);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r1, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, "", StringView(long1), StringView(long3)}));
}

TEST_F(PrestoIterativePartitioningSerializerTest, varbinaryWithNulls) {
  auto type = ROW({"b"}, {VARBINARY()});

  const std::string binary0("\x00\x01\x02", 3);
  const std::string binary1("ab\0cd", 5);
  const std::string binary3(StringView::kInlineSize + 6, '\x7f');

  std::vector<std::optional<std::string>> inputValues = {
      binary0,
      std::nullopt,
      binary1,
      std::string(),
      binary3,
  };

  auto serializer = makeSerializer(type, 2);
  serializer->append(
      makeRowVector(
          {"b"},
          {makeNullableFlatVector<std::string>(inputValues, VARBINARY())}),
      {0, 1, 1, 0, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 2);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r0, 0),
      (std::vector<std::optional<StringView>>{"", StringView(binary0)}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 3);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r1, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, StringView(binary1), StringView(binary3)}));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    dictionaryVarbinaryRoundTrip) {
  auto type = ROW({"b"}, {VARBINARY()});

  const std::string binary0("\x00\x01\x02", 3);
  const std::string binary1("ab\0cd", 5);
  const std::string binary2(StringView::kInlineSize + 6, '\x7f');

  auto base = makeNullableFlatVector<std::string>(
      {binary0, std::nullopt, binary1, std::string(), binary2}, VARBINARY());
  auto dictionary = BaseVector::wrapInDictionary(
      makeNulls({false, true, false, false, false, false}),
      makeIndices({4, 2, 1, 0, 3, 2}),
      6,
      base);

  auto serializer = makeSerializer(type, 2);
  serializer->append(makeRowVector({"b"}, {dictionary}), {0, 1, 0, 1, 0, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 3);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r0, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, "", StringView(binary2)}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 3);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r1, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, StringView(binary0), StringView(binary1)}));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    varbinaryMixedFlatConstantAcrossAppends) {
  auto type = ROW({"b"}, {VARBINARY()});

  const std::string binary0("\x00\x01\x02", 3);
  const std::string binary1("ab\0cd", 5);
  const std::string binary2("\x01\x02\x03\x04", 4);
  const std::string binary3(StringView::kInlineSize + 6, '\x7f');

  auto serializer = makeSerializer(type, 2);
  serializer->append(
      makeRowVector(
          {"b"},
          {makeNullableFlatVector<std::string>(
              {binary0, std::nullopt, binary1}, VARBINARY())}),
      {0, 1, 0});
  serializer->append(
      makeRowVector(
          {"b"}, {makeConstant<std::string>(binary2, 3, VARBINARY())}),
      {1, 0, 1});
  serializer->append(
      makeRowVector({"b"}, {makeNullConstant(TypeKind::VARBINARY, 2)}), {0, 1});
  serializer->append(
      makeRowVector(
          {"b"}, {makeFlatVector<std::string>({"", binary3}, VARBINARY())}),
      {1, 0});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 5);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r0, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt,
          StringView(binary0),
          StringView(binary2),
          StringView(binary1),
          StringView(binary3)}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 5);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r1, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt,
          std::nullopt,
          "",
          StringView(binary2),
          StringView(binary2)}));
}

TEST_F(
    PrestoIterativePartitioningSerializerTest,
    varcharConstantColumnAcrossAppends) {
  auto type = ROW({"s"}, {VARCHAR()});

  const std::string longValue =
      std::string(StringView::kInlineSize + 5, 'z') + "-tail";

  auto serializer = makeSerializer(type, 2);
  serializer->append(
      makeRowVector({"s"}, {makeConstant<std::string>("short", 3)}), {0, 1, 0});
  serializer->append(
      makeRowVector({"s"}, {makeConstant<std::string>(longValue, 3)}),
      {1, 0, 1});
  serializer->append(
      makeRowVector({"s"}, {makeNullConstant(TypeKind::VARCHAR, 2)}), {0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r0, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt, "short", "short", StringView(longValue)}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  EXPECT_EQ(
      sortedNullableValues<StringView>(r1, 0),
      (std::vector<std::optional<StringView>>{
          std::nullopt,
          "short",
          StringView(longValue),
          StringView(longValue)}));
}

// ── Scale and regression
// ───────────────────────────────────────────────────────

// 1024 partitions with random int64 values: verify every value reaches
// exactly the right partition and nothing is lost or duplicated.
TEST_F(PrestoIterativePartitioningSerializerTest, manyPartitionsRandom) {
  constexpr uint32_t kNumPartitions = 1024;
  constexpr int32_t kNumRows = 64'000;

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<int64_t> valueDist;
  std::uniform_int_distribution<uint32_t> partDist(0, kNumPartitions - 1);

  std::vector<int64_t> inputValues(kNumRows);
  std::vector<uint32_t> partitions(kNumRows);
  // expected[p] holds the sorted values assigned to partition p.
  std::vector<std::vector<int64_t>> expected(kNumPartitions);

  for (int i = 0; i < kNumRows; ++i) {
    inputValues[i] = valueDist(rng);
    partitions[i] = partDist(rng);
    expected[partitions[i]].push_back(inputValues[i]);
  }
  for (auto& v : expected) {
    std::sort(v.begin(), v.end());
  }

  auto type = ROW({"v"}, {BIGINT()});
  auto input = makeRowVector({"v"}, {makeFlatVector<int64_t>(inputValues)});

  auto serializer = makeSerializer(type, kNumPartitions);
  serializer->append(input, partitions);
  auto ioBufs = serializer->flush();

  // Every non-empty partition must have a page; empty partitions must not.
  for (uint32_t p = 0; p < kNumPartitions; ++p) {
    if (expected[p].empty()) {
      EXPECT_EQ(ioBufs.count(p), 0) << "partition " << p;
    } else {
      ASSERT_EQ(ioBufs.count(p), 1) << "partition " << p;
      auto result = deserialize(*ioBufs.at(p).first, type);
      ASSERT_EQ(result->size(), static_cast<int32_t>(expected[p].size()))
          << "partition " << p;
      EXPECT_EQ(sortedValues<int64_t>(result, 0), expected[p])
          << "partition " << p;
    }
  }
}

// 1024 partitions with random int64 values and ~25% nulls: verify every
// value and null reaches exactly the right partition in input order, and
// nothing is lost or duplicated.
TEST_F(
    PrestoIterativePartitioningSerializerTest,
    manyPartitionsRandomWithNulls) {
  constexpr uint32_t kNumPartitions = 1024;
  constexpr int32_t kNumRows = 64'000;
  constexpr int32_t kNullPct = 25;

  std::mt19937_64 rng(43);
  std::uniform_int_distribution<int64_t> valueDist;
  std::uniform_int_distribution<uint32_t> partDist(0, kNumPartitions - 1);
  std::uniform_int_distribution<int32_t> nullDist(0, 99);

  std::vector<std::optional<int64_t>> inputValues(kNumRows);
  std::vector<uint32_t> partitions(kNumRows);
  // expected[p] holds the sequence of (value-or-null) assigned to partition p
  // in input order.
  std::vector<std::vector<std::optional<int64_t>>> expected(kNumPartitions);

  for (int i = 0; i < kNumRows; ++i) {
    partitions[i] = partDist(rng);
    if (nullDist(rng) < kNullPct) {
      inputValues[i] = std::nullopt;
    } else {
      inputValues[i] = valueDist(rng);
    }
    expected[partitions[i]].push_back(inputValues[i]);
  }

  auto type = ROW({"v"}, {BIGINT()});
  auto input =
      makeRowVector({"v"}, {makeNullableFlatVector<int64_t>(inputValues)});

  auto serializer = makeSerializer(type, kNumPartitions);
  serializer->append(input, partitions);
  auto ioBufs = serializer->flush();

  // Partition rearranges values within each partition, so compare sorted.
  // std::optional<T> sorts with nullopt < any value, preserving null count.
  for (uint32_t p = 0; p < kNumPartitions; ++p) {
    if (expected[p].empty()) {
      EXPECT_EQ(ioBufs.count(p), 0) << "partition " << p;
    } else {
      ASSERT_EQ(ioBufs.count(p), 1) << "partition " << p;
      auto result = deserialize(*ioBufs.at(p).first, type);
      ASSERT_EQ(result->size(), static_cast<int32_t>(expected[p].size()))
          << "partition " << p;

      auto expectedSorted = expected[p];
      std::sort(expectedSorted.begin(), expectedSorted.end());

      auto actual = nullableValues<int64_t>(result, 0);
      std::sort(actual.begin(), actual.end());

      EXPECT_EQ(actual, expectedSorted) << "partition " << p;
    }
  }
}

// ── Checksum (CRC32)
// ──────────────────────────────────────────────────────

// Verify the checksum bit is set and a non-zero checksum is written when a
// PrestoOutputStreamListener factory is provided, and that the standard
// deserializer (which validates the checksum) accepts the page.
TEST_P(PrestoIterativePartitioningSerializerParamTest, checksumRoundTrip) {
  auto colType = GetParam();
  auto type = ROW({"a"}, {colType});
  auto col = BaseVector::create(colType, 6, pool_.get());
  col->setNull(1, true);
  col->setNull(4, true);

  auto serializer = makeSerializerWithListener(type, 2);
  serializer->append(makeRowVector({"a"}, {col}), {0, 1, 0, 1, 0, 1});
  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  for (auto& [partition, pageData] : ioBufs) {
    auto& iobuf = *pageData.first;
    EXPECT_NE(codecByte(iobuf) & kChecksumBitMask, 0)
        << "checksum bit must be set in codec byte";
    EXPECT_NE(checksumField(iobuf), 0) << "checksum field must be non-zero";
    // Deserializer validates the checksum internally; throws if wrong.
    auto result = deserialize(iobuf, type);
    EXPECT_GT(result->size(), 0);
  }
}

// ---------------------------------------------------------------------------
// Non-typed fixture (TEST_F) — lifecycle, structural, regression
// ---------------------------------------------------------------------------

// Regression: flushNulls previously wrote null bitmaps by obtaining a raw
// pointer via writePosition() then advancing the stream via seekp(). This
// assumed the pre-allocated IOBufOutputStream had a single contiguous range,
// but StreamArena::newRange caps each range at the size of one allocator run,
// which can be smaller than the requested size. seekp() then failed because
// the target position exceeded the end of the first (and only) range.
//
// Reproducing condition: 16 columns × 10'000 rows × 50% nulls in one
// partition generates enough output (~100 KB) to trigger the run-size cap.
TEST_F(
    PrestoIterativePartitioningSerializerTest,
    flushNullsBitmapManyColumnsLargeRowCount) {
  constexpr int32_t kNumCols = 16;
  constexpr int32_t kNumRows = 10'000;

  std::vector<std::string> names;
  std::vector<VectorPtr> children;
  names.reserve(kNumCols);
  children.reserve(kNumCols);

  for (int col = 0; col < kNumCols; ++col) {
    names.push_back(fmt::format("c{}", col));
    // Rows where (row % 2 == 0) are null; the rest hold (row * kNumCols + col).
    children.push_back(makeFlatVector<int64_t>(
        kNumRows,
        [col](auto row) { return static_cast<int64_t>(row * kNumCols + col); },
        [](auto row) { return (row % 2) == 0; }));
  }

  auto input = makeRowVector(names, children);
  auto rowType = std::static_pointer_cast<const RowType>(input->type());

  auto serializer = makeSerializer(rowType, 1);
  serializer->append(input, std::vector<uint32_t>(kNumRows, 0));
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 1);

  auto result = deserialize(*ioBufs.at(0).first, rowType);
  ASSERT_EQ(result->size(), kNumRows);

  for (int col = 0; col < kNumCols; ++col) {
    auto* flat = result->childAt(col)->as<FlatVector<int64_t>>();
    for (int row = 0; row < kNumRows; ++row) {
      if ((row % 2) == 0) {
        EXPECT_TRUE(result->childAt(col)->isNullAt(row))
            << "col=" << col << " row=" << row;
      } else {
        ASSERT_FALSE(result->childAt(col)->isNullAt(row))
            << "col=" << col << " row=" << row;
        EXPECT_EQ(
            flat->valueAt(row), static_cast<int64_t>(row * kNumCols + col))
            << "col=" << col << " row=" << row;
      }
    }
  }
}
