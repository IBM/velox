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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/serializers/PrestoHeader.h"
#include "velox/serializers/PrestoIterativePartitioningSerializer.h"
#include "velox/serializers/PrestoSerializerDeserializationUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::serializer::presto;
using namespace facebook::velox::test;

class PrestoIterativePartitioningSerializerTest : public ::testing::Test,
                                                  public VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    if (!isRegisteredVectorSerde()) {
      PrestoVectorSerde::registerVectorSerde();
    }
  }

  /// Deserializes an IOBuf produced by PartitioningSerializer::flush().
  RowVectorPtr deserialize(
      folly::IOBuf& iobuf,
      const RowTypePtr& type,
      const SerdeOpts* opts = {}) {
    auto ranges = byteRangesFromIOBuf(&iobuf);
    BufferInputStream stream(std::move(ranges));
    RowVectorPtr result;
    serde_.deserialize(&stream, pool_.get(), type, &result, opts);
    return result;
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

  /// Builds a PrestoIterativePartitioningSerializer with default serde options.
  std::unique_ptr<PrestoIterativePartitioningSerializer> makeSerializer(
      const RowTypePtr& type,
      uint32_t numPartitions,
      const SerdeOpts& opts = {}) {
    return std::make_unique<PrestoIterativePartitioningSerializer>(
        type, numPartitions, opts, pool_.get());
  }

  PrestoVectorSerde serde_;
};

// ── Routing ──────────────────────────────────────────────────────────────────

// Single append, two equal-sized partitions.
TEST_F(PrestoIterativePartitioningSerializerTest, basicTwoPartitions) {
  auto type = ROW({"a"}, {BIGINT()});
  auto input =
      makeRowVector({"a"}, {makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60})});

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

  ASSERT_EQ(p0->size(), 3);
  ASSERT_EQ(p1->size(), 3);

  EXPECT_EQ(sortedValues<int64_t>(p0, 0), (std::vector<int64_t>{10, 30, 50}));
  EXPECT_EQ(sortedValues<int64_t>(p1, 0), (std::vector<int64_t>{20, 40, 60}));
}

// All rows routed to one non-zero partition; other partitions are absent.
TEST_F(PrestoIterativePartitioningSerializerTest, allRowsToOnePartition) {
  auto type = ROW({"x"}, {INTEGER()});
  auto input = makeRowVector({"x"}, {makeFlatVector<int32_t>({1, 2, 3, 4, 5})});

  auto serializer = makeSerializer(type, 4);
  serializer->append(input, {2, 2, 2, 2, 2});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 1);
  ASSERT_TRUE(ioBufs.count(2));

  auto result = deserialize(*ioBufs.at(2).first, type);
  ASSERT_EQ(result->size(), 5);
  EXPECT_EQ(
      sortedValues<int32_t>(result, 0), (std::vector<int32_t>{1, 2, 3, 4, 5}));
}

// Single partition (numPartitions=1): all rows go to partition 0.
TEST_F(PrestoIterativePartitioningSerializerTest, singlePartition) {
  auto type = ROW({"a"}, {BIGINT()});
  auto input = makeRowVector({"a"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5})});

  auto serializer = makeSerializer(type, 1);
  serializer->append(input, std::vector<uint32_t>(5, 0));
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 1);
  auto result = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(result->size(), 5);
  EXPECT_EQ(
      sortedValues<int64_t>(result, 0), (std::vector<int64_t>{1, 2, 3, 4, 5}));
}

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

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 3);

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

// Multiple columns: each is serialized independently by flushRowChildren.
TEST_F(PrestoIterativePartitioningSerializerTest, multipleColumns) {
  auto type = ROW({"a", "b"}, {INTEGER(), BIGINT()});
  auto input = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int32_t>({1, 2, 3, 4}),
       makeFlatVector<int64_t>({10, 20, 30, 40})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 2);
  EXPECT_THAT(sortedValues<int32_t>(r0, 0), testing::ElementsAre(1, 2));
  EXPECT_THAT(sortedValues<int64_t>(r0, 1), testing::ElementsAre(10, 20));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_THAT(sortedValues<int32_t>(r1, 0), testing::ElementsAre(3, 4));
  EXPECT_THAT(sortedValues<int64_t>(r1, 1), testing::ElementsAre(30, 40));
}

// ── Empty state
// ───────────────────────────────────────────────────────────────

// Flushing an empty serializer returns an empty map.
TEST_F(PrestoIterativePartitioningSerializerTest, flushEmpty) {
  auto type = ROW({"a"}, {BIGINT()});
  auto serializer = makeSerializer(type, 3);
  EXPECT_TRUE(serializer->flush().empty());
}

// Appending an empty RowVector produces no ioBufs on flush.
TEST_F(PrestoIterativePartitioningSerializerTest, appendEmptyVector) {
  auto type = ROW({"a"}, {BIGINT()});
  auto serializer = makeSerializer(type, 2);
  serializer->append(makeRowVector({"a"}, {makeFlatVector<int64_t>({})}), {});
  EXPECT_TRUE(serializer->flush().empty());
}

// ── Null handling
// ─────────────────────────────────────────────────────────────

// Nulls appear only in one partition; the other partition is null-free.
TEST_F(PrestoIterativePartitioningSerializerTest, nullsInOnePartition) {
  auto type = ROW({"a"}, {BIGINT()});
  using opt = std::optional<int64_t>;
  // Rows 0,1,2 → partition 0; rows 3,4 → partition 1.
  // Partition 0 has a null at position 1; partition 1 has no nulls.
  auto input = makeRowVector(
      {"a"}, {makeNullableFlatVector<int64_t>({10, std::nullopt, 30, 40, 50})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 3);
  EXPECT_THAT(
      nullableValues<int64_t>(r0, 0),
      testing::ElementsAre(opt{10}, std::nullopt, opt{30}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_THAT(
      nullableValues<int64_t>(r1, 0), testing::ElementsAre(opt{40}, opt{50}));
}

// Both partitions contain nulls.
TEST_F(PrestoIterativePartitioningSerializerTest, nullsInBothPartitions) {
  auto type = ROW({"a"}, {BIGINT()});
  using opt = std::optional<int64_t>;
  // Rows 0,1 → partition 0; rows 2,3 → partition 1.
  // Partition 0: [10, null]; partition 1: [null, 40].
  auto input = makeRowVector(
      {"a"},
      {makeNullableFlatVector<int64_t>({10, std::nullopt, std::nullopt, 40})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  EXPECT_THAT(
      nullableValues<int64_t>(r0, 0),
      testing::ElementsAre(opt{10}, std::nullopt));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  EXPECT_THAT(
      nullableValues<int64_t>(r1, 0),
      testing::ElementsAre(std::nullopt, opt{40}));
}

// All rows in one partition are null.
TEST_F(PrestoIterativePartitioningSerializerTest, allNullsInPartition) {
  auto type = ROW({"a"}, {BIGINT()});
  using opt = std::optional<int64_t>;
  // Partition 0: two nulls. Partition 1: one non-null.
  auto input = makeRowVector(
      {"a"},
      {makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, 30})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  EXPECT_THAT(
      nullableValues<int64_t>(r0, 0),
      testing::ElementsAre(std::nullopt, std::nullopt));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  EXPECT_THAT(nullableValues<int64_t>(r1, 0), testing::ElementsAre(opt{30}));
}

// Nulls contributed by different appends to the same partition, exercising
// carry-over between vectors.
TEST_F(PrestoIterativePartitioningSerializerTest, nullsAcrossMultipleAppends) {
  auto type = ROW({"a"}, {BIGINT()});
  using opt = std::optional<int64_t>;
  auto serializer = makeSerializer(type, 2);

  // Append 1: rows 0,1 → partition 0; row 2 → partition 1.
  // Partition 0 gets [10, null]; partition 1 gets [30].
  serializer->append(
      makeRowVector(
          {"a"}, {makeNullableFlatVector<int64_t>({10, std::nullopt, 30})}),
      {0, 0, 1});

  // Append 2: row 0 → partition 0; row 1 → partition 1.
  // Partition 0 gets [null]; partition 1 gets [50].
  serializer->append(
      makeRowVector(
          {"a"}, {makeNullableFlatVector<int64_t>({std::nullopt, 50})}),
      {0, 1});

  auto ioBufs = serializer->flush();
  ASSERT_EQ(ioBufs.size(), 2);

  // Partition 0 should have [10, null] from append 1 + [null] from append 2.
  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 3);
  EXPECT_THAT(
      nullableValues<int64_t>(r0, 0),
      testing::ElementsAre(opt{10}, std::nullopt, std::nullopt));

  // Partition 1 should have [30] from append 1 + [50] from append 2.
  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_THAT(
      nullableValues<int64_t>(r1, 0), testing::ElementsAre(opt{30}, opt{50}));
}

// Partition boundary falls in the middle of a null-bitmap byte, exercising the
// bit-extraction carry-over logic.
TEST_F(PrestoIterativePartitioningSerializerTest, nullsUnalignedBoundary) {
  auto type = ROW({"a"}, {BIGINT()});
  using opt = std::optional<int64_t>;
  // 5 rows → partition 0, 4 rows → partition 1. The boundary at bit 5 is
  // inside the first byte of the null bitmap.
  auto input = makeRowVector(
      {"a"},
      {makeNullableFlatVector<int64_t>(
          {10,
           std::nullopt,
           30,
           std::nullopt,
           50,
           std::nullopt,
           70,
           std::nullopt,
           90})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 0, 0, 0, 1, 1, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 5);
  EXPECT_THAT(
      nullableValues<int64_t>(r0, 0),
      testing::ElementsAre(
          opt{10}, std::nullopt, opt{30}, std::nullopt, opt{50}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 4);
  EXPECT_THAT(
      nullableValues<int64_t>(r1, 0),
      testing::ElementsAre(std::nullopt, opt{70}, std::nullopt, opt{90}));
}

// ── BOOLEAN type
// ────────────────────────────────────────────────────────────── BOOLEAN
// columns use bit-packed FlatVector<bool> storage; the serializer writes each
// non-null value as a kByteArray byte (0x00 or 0x01). The tests below verify
// correct encoding, null handling, and partition routing.

// Two partitions, no nulls: {T,F,T,F,T,F} alternating, even→p0, odd→p1.
TEST_F(PrestoIterativePartitioningSerializerTest, booleanNoNulls) {
  auto type = ROW({"b"}, {BOOLEAN()});
  auto input = makeRowVector(
      {"b"}, {makeFlatVector<bool>({true, false, true, false, true, false})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 1, 0, 1, 0, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 3);
  EXPECT_THAT(
      nullableValues<bool>(r0, 0), testing::ElementsAre(true, true, true));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 3);
  EXPECT_THAT(
      nullableValues<bool>(r1, 0), testing::ElementsAre(false, false, false));
}

// Nulls mixed into both partitions.
TEST_F(PrestoIterativePartitioningSerializerTest, booleanWithNulls) {
  auto type = ROW({"b"}, {BOOLEAN()});
  using opt = std::optional<bool>;
  // p0: [T, null, F]; p1: [null, T].
  auto input = makeRowVector(
      {"b"},
      {makeNullableFlatVector<bool>(
          {true, std::nullopt, false, std::nullopt, true})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  EXPECT_THAT(
      nullableValues<bool>(r0, 0),
      testing::ElementsAre(opt{true}, std::nullopt, opt{false}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  EXPECT_THAT(
      nullableValues<bool>(r1, 0),
      testing::ElementsAre(std::nullopt, opt{true}));
}

// All values null in one partition; non-null values in the other.
TEST_F(PrestoIterativePartitioningSerializerTest, booleanAllNullsInPartition) {
  auto type = ROW({"b"}, {BOOLEAN()});
  using opt = std::optional<bool>;
  auto input = makeRowVector(
      {"b"},
      {makeNullableFlatVector<bool>(
          {std::nullopt, std::nullopt, true, false})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  EXPECT_THAT(
      nullableValues<bool>(r0, 0),
      testing::ElementsAre(std::nullopt, std::nullopt));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  EXPECT_THAT(
      nullableValues<bool>(r1, 0), testing::ElementsAre(opt{true}, opt{false}));
}

// Multiple BOOLEAN columns: each is serialized and deserialized independently.
TEST_F(PrestoIterativePartitioningSerializerTest, booleanMultipleColumns) {
  auto type = ROW({"x", "y"}, {BOOLEAN(), BOOLEAN()});
  using opt = std::optional<bool>;
  // 4 rows, 2 partitions.
  auto input = makeRowVector(
      {"x", "y"},
      {makeNullableFlatVector<bool>({true, std::nullopt, false, true}),
       makeNullableFlatVector<bool>(
           {std::nullopt, false, true, std::nullopt})});

  auto serializer = makeSerializer(type, 2);
  serializer->append(input, {0, 0, 1, 1});
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  auto r0 = deserialize(*ioBufs.at(0).first, type);
  ASSERT_EQ(r0->size(), 2);
  EXPECT_THAT(
      nullableValues<bool>(r0, 0),
      testing::ElementsAre(opt{true}, std::nullopt));
  EXPECT_THAT(
      nullableValues<bool>(r0, 1),
      testing::ElementsAre(std::nullopt, opt{false}));

  auto r1 = deserialize(*ioBufs.at(1).first, type);
  ASSERT_EQ(r1->size(), 2);
  EXPECT_THAT(
      nullableValues<bool>(r1, 0), testing::ElementsAre(opt{false}, opt{true}));
  EXPECT_THAT(
      nullableValues<bool>(r1, 1),
      testing::ElementsAre(opt{true}, std::nullopt));
}

// Verify compressed page header bits and round-trip deserialization.
TEST_F(PrestoIterativePartitioningSerializerTest, compressedRoundTrip) {
  constexpr int32_t kNumRows = 5'000;

  SerdeOpts opts;
  opts.compressionKind = common::CompressionKind::CompressionKind_ZLIB;
  opts.minCompressionRatio = 0.99;

  std::vector<int64_t> values(kNumRows, 7);
  std::vector<uint32_t> partitions(kNumRows);
  for (int32_t i = 0; i < kNumRows; ++i) {
    partitions[i] = i % 2;
  }

  auto type = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  auto input = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>(values), makeFlatVector<int64_t>(values)});

  auto serializer = makeSerializer(type, 2, opts);
  serializer->append(input, partitions);
  auto ioBufs = serializer->flush();

  ASSERT_EQ(ioBufs.size(), 2);

  const std::vector<int64_t> expected(kNumRows / 2, 7);
  for (uint32_t partition = 0; partition < 2; ++partition) {
    auto ranges = byteRangesFromIOBuf(ioBufs.at(partition).first.get());
    BufferInputStream stream(std::move(ranges));
    auto maybeHeader = serializer::presto::detail::PrestoHeader::read(&stream);
    ASSERT_TRUE(maybeHeader.hasValue());

    const auto header = maybeHeader.value();
    EXPECT_TRUE(
        serializer::presto::detail::isCompressedBitSet(header.pageCodecMarker));
    EXPECT_LT(header.compressedSize, header.uncompressedSize);

    auto result = deserialize(*ioBufs.at(partition).first, type, &opts);
    ASSERT_EQ(result->size(), kNumRows / 2);
    EXPECT_EQ(sortedValues<int64_t>(result, 0), expected);
    EXPECT_EQ(sortedValues<int64_t>(result, 1), expected);
  }
}

// ── Lifecycle
// ─────────────────────────────────────────────────────────────────

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

// ── Scale and regression
// ──────────────────────────────────────────────────────

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
    children.push_back(
        makeFlatVector<int64_t>(
            kNumRows,
            [col](auto row) {
              return static_cast<int64_t>(row * kNumCols + col);
            },
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
