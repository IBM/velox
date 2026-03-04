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

#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

#include <gtest/gtest.h>
#include <cmath>
#include <limits>
#include <numeric>

#include "velox/common/memory/MemoryPool.h"
#include "velox/dwio/common/FormatData.h"
#include "velox/dwio/common/TypeWithId.h"

namespace facebook::velox::dwio::common {
namespace {

TEST(IsDenseTest, empty) {
  const RowSet rows(nullptr, nullptr);
  EXPECT_TRUE(isDense(rows));
}

TEST(IsDenseTest, singleElement) {
  const std::vector<int32_t> data{0};
  const RowSet rows(data.data(), data.size());
  EXPECT_TRUE(isDense(rows));
}

TEST(IsDenseTest, contiguousFromZero) {
  const std::vector<int32_t> data{0, 1, 2, 3, 4};
  const RowSet rows(data.data(), data.size());
  EXPECT_TRUE(isDense(rows));
}

TEST(IsDenseTest, sparseRows) {
  const std::vector<int32_t> data{0, 2, 4};
  const RowSet rows(data.data(), data.size());
  EXPECT_FALSE(isDense(rows));
}

TEST(IsDenseTest, startingFromNonZero) {
  const std::vector<int32_t> data{1, 2, 3};
  const RowSet rows(data.data(), data.size());
  EXPECT_FALSE(isDense(rows));
}

TEST(IsDenseTest, singleNonZeroElement) {
  const std::vector<int32_t> data{5};
  const RowSet rows(data.data(), data.size());
  EXPECT_FALSE(isDense(rows));
}

// Minimal FormatData stub for testing SelectiveColumnReader in isolation.
class StubFormatData : public FormatData {
 public:
  void readNulls(
      vector_size_t /*numValues*/,
      const uint64_t* /*incomingNulls*/,
      BufferPtr& nulls,
      bool /*nullsOnly*/) override {
    nulls = nullptr;
  }
  uint64_t skipNulls(uint64_t numValues, bool /*nullsOnly*/) override {
    return numValues;
  }
  uint64_t skip(uint64_t numValues) override {
    return numValues;
  }
  bool hasNulls() const override {
    return false;
  }
  dwio::common::PositionProvider seekToRowGroup(int64_t /*index*/) override {
    static std::vector<uint64_t> empty;
    return dwio::common::PositionProvider(empty);
  }
  void filterRowGroups(
      const velox::common::ScanSpec& /*scanSpec*/,
      uint64_t /*rowsPerRowGroup*/,
      const StatsContext& /*writerContext*/,
      FilterRowGroupsResult& /*result*/) override {}
};

// Minimal FormatParams stub that produces a StubFormatData.
class StubFormatParams : public FormatParams {
 public:
  StubFormatParams(memory::MemoryPool& pool, ColumnReaderStatistics& stats)
      : FormatParams(pool, stats) {}

  std::unique_ptr<FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& /*type*/,
      const velox::common::ScanSpec& /*scanSpec*/) override {
    return std::make_unique<StubFormatData>();
  }
};

// Concrete subclass that exposes getFlatValues and internal state for testing.
class TestColumnReader : public SelectiveColumnReader {
 public:
  TestColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec)
      : SelectiveColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {}

  void read(
      int64_t /*offset*/,
      const RowSet& /*rows*/,
      const uint64_t* /*incomingNulls*/) override {}

  void getValues(const RowSet& /*rows*/, VectorPtr* /*result*/) override {}

  /// Populate the internal values buffer with source data of type T.
  /// Sets valueSize_, numValues_, mayGetValues_, and inputRows_.
  /// @note rowNumbers must remain live until getFlatValues is called, as
  /// inputRows_ stores a non-owning reference.
  template <typename T>
  void setupValues(
      const std::vector<T>& data,
      const std::vector<int32_t>& rowNumbers) {
    VELOX_CHECK_EQ(data.size(), rowNumbers.size());
    const auto n = static_cast<vector_size_t>(data.size());
    ensureValuesCapacity<T>(n);
    std::memcpy(rawValues_, data.data(), n * sizeof(T));
    numValues_ = n;
    valueSize_ = sizeof(T);
    mayGetValues_ = true;
    allNull_ = false;
    inputRows_ = RowSet(rowNumbers.data(), rowNumbers.size());
  }

  /// Populate internal values and mark elements null according to the mask.
  template <typename T>
  void setupValuesWithNulls(
      const std::vector<T>& data,
      const std::vector<bool>& nulls,
      const std::vector<int32_t>& rowNumbers) {
    VELOX_CHECK_EQ(data.size(), nulls.size());
    setupValues(data, rowNumbers);
    const auto n = static_cast<vector_size_t>(data.size());
    anyNulls_ = true;
    resultNulls_ = AlignedBuffer::allocate<bool>(
        n + simd::kPadding * 8, pool_, bits::kNotNull);
    rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
    for (int32_t i = 0; i < n; ++i) {
      bits::setBit(rawResultNulls_, i, !nulls[i]);
    }
  }

  /// Force the all-null short-circuit path in convertDateToTimestampValues.
  void setAllNull(bool allNull) {
    allNull_ = allNull;
  }

  // Inspectors for post-call state assertions. Kept in the test subclass so the
  // production class's API is unchanged.
  int8_t valueSize() const {
    return valueSize_;
  }
  bool mayGetValues() const {
    return mayGetValues_;
  }
  vector_size_t numValues() const {
    return numValues_;
  }

  using SelectiveColumnReader::convertDateToTimestampValues;
  using SelectiveColumnReader::getFlatValues;
};

class GetFlatValuesTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("GetFlatValuesTest");
    stats_ = std::make_unique<ColumnReaderStatistics>();
    params_ = std::make_unique<StubFormatParams>(*pool_, *stats_);
    scanSpec_ = std::make_unique<velox::common::ScanSpec>("test");
    scanSpec_->setProjectOut(true);
  }

  std::unique_ptr<TestColumnReader> makeReader(
      const TypePtr& requestedType) const {
    auto fileType = TypeWithId::create(requestedType);
    return std::make_unique<TestColumnReader>(
        requestedType, std::move(fileType), *params_, *scanSpec_);
  }

  /// Run getFlatValues and verify each element equals static_cast<Target>(src).
  /// When sparseRows is provided, only those rows are selected from data.
  template <typename Source, typename Target>
  void testConversion(
      const TypePtr& type,
      const std::vector<Source>& data,
      const std::vector<int32_t>& sparseRows = {}) {
    auto reader = makeReader(type);
    std::vector<int32_t> allRows(data.size());
    std::iota(allRows.begin(), allRows.end(), 0);
    reader->setupValues(data, allRows);

    const auto& selectedRows = sparseRows.empty() ? allRows : sparseRows;
    const RowSet rows(selectedRows.data(), selectedRows.size());
    VectorPtr result;
    reader->getFlatValues<Source, Target>(rows, &result, type, true);

    auto* flat = result->as<FlatVector<Target>>();
    ASSERT_NE(flat, nullptr);
    ASSERT_EQ(flat->size(), selectedRows.size());
    for (size_t i = 0; i < selectedRows.size(); ++i) {
      EXPECT_EQ(flat->valueAt(i), static_cast<Target>(data[selectedRows[i]]))
          << "Mismatch at index " << i;
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<ColumnReaderStatistics> stats_;
  std::unique_ptr<StubFormatParams> params_;
  std::unique_ptr<velox::common::ScanSpec> scanSpec_;
};

// Cross-domain upcast conversions (different sizes, valid via
// upcastScalarValues).

TEST_F(GetFlatValuesTest, int16ToFloat) {
  testConversion<int16_t, float>(REAL(), {1, -32'000, 0, 127, 32'767});
  testConversion<int16_t, float>(REAL(), {1, -32'000, 0, 127, 32'767}, {0, 3});
}

TEST_F(GetFlatValuesTest, int32ToDouble) {
  testConversion<int32_t, double>(
      DOUBLE(), {1, -999'999, 2'147'483'647, 0, -2'147'483'647});
  testConversion<int32_t, double>(
      DOUBLE(), {1, -999'999, 2'147'483'647, 0, -2'147'483'647}, {1, 2, 4});
}

TEST_F(GetFlatValuesTest, int16ToDouble) {
  testConversion<int16_t, double>(DOUBLE(), {0, 1, -1, 32'767, -32'768});
  testConversion<int16_t, double>(
      DOUBLE(), {0, 1, -1, 32'767, -32'768}, {0, 4});
}

// Same-domain conversions as regression guards.

TEST_F(GetFlatValuesTest, int32ToInt32) {
  testConversion<int32_t, int32_t>(INTEGER(), {0, 1, -1, 42, 2'147'483'647});
  testConversion<int32_t, int32_t>(
      INTEGER(), {0, 1, -1, 42, 2'147'483'647}, {2, 3});
}

TEST_F(GetFlatValuesTest, int16ToInt32) {
  testConversion<int16_t, int32_t>(INTEGER(), {0, 1, -1, 32'767, -32'768});
  testConversion<int16_t, int32_t>(
      INTEGER(), {0, 1, -1, 32'767, -32'768}, {1, 4});
}

TEST_F(GetFlatValuesTest, int32ToInt16) {
  testConversion<int32_t, int16_t>(SMALLINT(), {0, 1, -1, 127, -128});
  testConversion<int32_t, int16_t>(SMALLINT(), {0, 1, -1, 127, -128}, {0, 2});
}

TEST_F(GetFlatValuesTest, int64ToInt16) {
  testConversion<int64_t, int16_t>(SMALLINT(), {0, 1, -1, 127, -128});
  testConversion<int64_t, int16_t>(SMALLINT(), {0, 1, -1, 127, -128}, {3, 4});
}

TEST_F(GetFlatValuesTest, int32ToInt8) {
  testConversion<int32_t, int8_t>(TINYINT(), {0, 1, -1, 42, -100});
  testConversion<int32_t, int8_t>(TINYINT(), {0, 1, -1, 42, -100}, {1, 3, 4});
}

TEST_F(GetFlatValuesTest, int16ToInt8) {
  testConversion<int16_t, int8_t>(TINYINT(), {0, 1, -1, 42, -100});
  testConversion<int16_t, int8_t>(TINYINT(), {0, 1, -1, 42, -100}, {0, 4});
}

TEST_F(GetFlatValuesTest, int32ToInt64) {
  testConversion<int32_t, int64_t>(
      BIGINT(), {0, 1, -1, 2'147'483'647, -2'147'483'648});
  testConversion<int32_t, int64_t>(
      BIGINT(), {0, 1, -1, 2'147'483'647, -2'147'483'648}, {0, 2, 3});
}

TEST_F(GetFlatValuesTest, int16ToInt64) {
  testConversion<int16_t, int64_t>(BIGINT(), {0, 1, -1, 32'767, -32'768});
  testConversion<int16_t, int64_t>(
      BIGINT(), {0, 1, -1, 32'767, -32'768}, {2, 4});
}

// HUGEINT widening conversions for Parquet type widening.

TEST_F(GetFlatValuesTest, int128ToInt128) {
  testConversion<int128_t, int128_t>(DECIMAL(38, 0), {0, 1, -1, 1'000'000});
  testConversion<int128_t, int128_t>(
      DECIMAL(38, 0), {0, 1, -1, 1'000'000}, {1, 3});
}

TEST_F(GetFlatValuesTest, int64ToInt128) {
  testConversion<int64_t, int128_t>(
      DECIMAL(38, 0),
      {0, 1, -1, 9'223'372'036'854'775'807LL, -9'223'372'036'854'775'807LL});
  testConversion<int64_t, int128_t>(
      DECIMAL(38, 0),
      {0, 1, -1, 9'223'372'036'854'775'807LL, -9'223'372'036'854'775'807LL},
      {0, 3, 4});
}

TEST_F(GetFlatValuesTest, int32ToInt128) {
  testConversion<int32_t, int128_t>(
      DECIMAL(38, 0), {0, 1, -1, 2'147'483'647, -2'147'483'648});
  testConversion<int32_t, int128_t>(
      DECIMAL(38, 0), {0, 1, -1, 2'147'483'647, -2'147'483'648}, {1, 4});
}

TEST_F(GetFlatValuesTest, int16ToInt128) {
  testConversion<int16_t, int128_t>(
      DECIMAL(38, 0), {0, 1, -1, 32'767, -32'768});
  testConversion<int16_t, int128_t>(
      DECIMAL(38, 0), {0, 1, -1, 32'767, -32'768}, {0, 2, 3});
}

// Regression guards for existing same-domain paths affected by template
// changes.

TEST_F(GetFlatValuesTest, int64ToInt64) {
  testConversion<int64_t, int64_t>(
      BIGINT(),
      {0, 1, -1, 9'223'372'036'854'775'807LL, -9'223'372'036'854'775'807LL});
  testConversion<int64_t, int64_t>(
      BIGINT(),
      {0, 1, -1, 9'223'372'036'854'775'807LL, -9'223'372'036'854'775'807LL},
      {0, 4});
}

TEST_F(GetFlatValuesTest, int64ToInt32) {
  testConversion<int64_t, int32_t>(INTEGER(), {0, 1, -1, 42, -100});
  testConversion<int64_t, int32_t>(INTEGER(), {0, 1, -1, 42, -100}, {2, 3});
}

TEST_F(GetFlatValuesTest, int16ToInt16) {
  testConversion<int16_t, int16_t>(SMALLINT(), {0, 1, -1, 32'767, -32'768});
  testConversion<int16_t, int16_t>(
      SMALLINT(), {0, 1, -1, 32'767, -32'768}, {1, 2, 4});
}

// ByteRle conversions (int8_t source).

TEST_F(GetFlatValuesTest, int8ToInt8) {
  testConversion<int8_t, int8_t>(TINYINT(), {0, 1, -1, 127, -128});
  testConversion<int8_t, int8_t>(TINYINT(), {0, 1, -1, 127, -128}, {0, 2, 4});
}

TEST_F(GetFlatValuesTest, int8ToInt16) {
  testConversion<int8_t, int16_t>(SMALLINT(), {0, 1, -1, 127, -128});
  testConversion<int8_t, int16_t>(SMALLINT(), {0, 1, -1, 127, -128}, {1, 3});
}

TEST_F(GetFlatValuesTest, int8ToInt32) {
  testConversion<int8_t, int32_t>(INTEGER(), {0, 1, -1, 127, -128});
  testConversion<int8_t, int32_t>(INTEGER(), {0, 1, -1, 127, -128}, {2, 4});
}

TEST_F(GetFlatValuesTest, int8ToInt64) {
  testConversion<int8_t, int64_t>(BIGINT(), {0, 1, -1, 127, -128});
  testConversion<int8_t, int64_t>(BIGINT(), {0, 1, -1, 127, -128}, {0, 3, 4});
}

// Unsigned integer conversions.

TEST_F(GetFlatValuesTest, uint8ToUint8) {
  testConversion<uint8_t, uint8_t>(TINYINT(), {0, 1, 42, 255});
  testConversion<uint8_t, uint8_t>(TINYINT(), {0, 1, 42, 255}, {1, 3});
}

TEST_F(GetFlatValuesTest, uint16ToUint16) {
  testConversion<uint16_t, uint16_t>(SMALLINT(), {0, 1, 42, 65'535});
  testConversion<uint16_t, uint16_t>(SMALLINT(), {0, 1, 42, 65'535}, {0, 2});
}

TEST_F(GetFlatValuesTest, uint32ToUint8) {
  testConversion<uint32_t, uint8_t>(TINYINT(), {0, 1, 42, 200});
  testConversion<uint32_t, uint8_t>(TINYINT(), {0, 1, 42, 200}, {2, 3});
}

TEST_F(GetFlatValuesTest, uint32ToUint16) {
  testConversion<uint32_t, uint16_t>(SMALLINT(), {0, 1, 42, 60'000});
  testConversion<uint32_t, uint16_t>(SMALLINT(), {0, 1, 42, 60'000}, {0, 3});
}

TEST_F(GetFlatValuesTest, uint32ToUint32) {
  testConversion<uint32_t, uint32_t>(INTEGER(), {0, 1, 42, 4'294'967'295U});
  testConversion<uint32_t, uint32_t>(
      INTEGER(), {0, 1, 42, 4'294'967'295U}, {1, 2});
}

TEST_F(GetFlatValuesTest, uint32ToUint64) {
  testConversion<uint32_t, uint64_t>(BIGINT(), {0, 1, 42, 4'294'967'295U});
  testConversion<uint32_t, uint64_t>(
      BIGINT(), {0, 1, 42, 4'294'967'295U}, {0, 3});
}

TEST_F(GetFlatValuesTest, uint64ToUint64) {
  testConversion<uint64_t, uint64_t>(BIGINT(), {0, 1, 42, 1'000'000'000ULL});
  testConversion<uint64_t, uint64_t>(
      BIGINT(), {0, 1, 42, 1'000'000'000ULL}, {1, 3});
}

TEST_F(GetFlatValuesTest, uint64ToUint128) {
  testConversion<uint64_t, uint128_t>(
      DECIMAL(38, 0), {0, 1, 42, 1'000'000'000ULL});
  testConversion<uint64_t, uint128_t>(
      DECIMAL(38, 0), {0, 1, 42, 1'000'000'000ULL}, {0, 2, 3});
}

TEST_F(GetFlatValuesTest, uint128ToUint128) {
  testConversion<uint128_t, uint128_t>(DECIMAL(38, 0), {0, 1, 42, 1'000'000});
  testConversion<uint128_t, uint128_t>(
      DECIMAL(38, 0), {0, 1, 42, 1'000'000}, {1, 2});
}

// Floating-point conversions.

TEST_F(GetFlatValuesTest, floatToFloat) {
  testConversion<float, float>(REAL(), {0.0f, 1.5f, -3.14f, 1e10f});
  testConversion<float, float>(REAL(), {0.0f, 1.5f, -3.14f, 1e10f}, {0, 2});
}

TEST_F(GetFlatValuesTest, floatToDouble) {
  testConversion<float, double>(DOUBLE(), {0.0f, 1.5f, -3.14f, 1e10f});
  testConversion<float, double>(DOUBLE(), {0.0f, 1.5f, -3.14f, 1e10f}, {1, 3});
}

TEST_F(GetFlatValuesTest, doubleToDouble) {
  testConversion<double, double>(DOUBLE(), {0.0, 1.5, -3.14, 1e100});
  testConversion<double, double>(DOUBLE(), {0.0, 1.5, -3.14, 1e100}, {0, 3});
}

TEST_F(GetFlatValuesTest, floatToDoubleSpecialValues) {
  auto reader = makeReader(DOUBLE());
  std::vector<float> data = {
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<float>::infinity(),
      -std::numeric_limits<float>::infinity(),
      -0.0f,
      std::numeric_limits<float>::denorm_min()};
  std::vector<int32_t> rowNums(data.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValues(data, rowNums);

  const RowSet rows(rowNums.data(), rowNums.size());
  VectorPtr result;
  reader->getFlatValues<float, double>(rows, &result, DOUBLE(), true);

  auto* flat = result->as<FlatVector<double>>();
  ASSERT_NE(flat, nullptr);
  ASSERT_EQ(flat->size(), data.size());
  EXPECT_TRUE(std::isnan(flat->valueAt(0)));
  EXPECT_EQ(flat->valueAt(1), std::numeric_limits<double>::infinity());
  EXPECT_EQ(flat->valueAt(2), -std::numeric_limits<double>::infinity());
  EXPECT_DOUBLE_EQ(flat->valueAt(3), -0.0);
  EXPECT_TRUE(std::signbit(flat->valueAt(3)));
  EXPECT_DOUBLE_EQ(
      flat->valueAt(4),
      static_cast<double>(std::numeric_limits<float>::denorm_min()));
}

// Boundary condition: single-element input.

TEST_F(GetFlatValuesTest, singleElement) {
  testConversion<int32_t, int64_t>(BIGINT(), {42});
}

// Large-scale conversion: 1024 values with dense and sparse rows.

TEST_F(GetFlatValuesTest, largeScaleInt32ToDouble) {
  constexpr int kSize = 1024;
  std::vector<int32_t> data(kSize);
  for (int i = 0; i < kSize; ++i) {
    data[i] = i * 7 - 3000;
  }
  // Dense.
  testConversion<int32_t, double>(DOUBLE(), data);
  // Sparse: every 3rd row.
  std::vector<int32_t> sparse;
  for (int i = 0; i < kSize; i += 3) {
    sparse.push_back(i);
  }
  testConversion<int32_t, double>(DOUBLE(), data, sparse);
}

TEST_F(GetFlatValuesTest, largeScaleInt64ToInt128) {
  constexpr int kSize = 1024;
  std::vector<int64_t> data(kSize);
  for (int i = 0; i < kSize; ++i) {
    data[i] = static_cast<int64_t>(i) * 123'456'789LL - 50'000'000'000LL;
  }
  testConversion<int64_t, int128_t>(DECIMAL(38, 0), data);
  // Sparse: odd indices.
  std::vector<int32_t> sparse;
  for (int i = 1; i < kSize; i += 2) {
    sparse.push_back(i);
  }
  testConversion<int64_t, int128_t>(DECIMAL(38, 0), data, sparse);
}

TEST_F(GetFlatValuesTest, largeScaleInt32ToInt16) {
  constexpr int kSize = 1024;
  std::vector<int32_t> data(kSize);
  for (int i = 0; i < kSize; ++i) {
    data[i] = (i % 256) - 128;
  }
  testConversion<int32_t, int16_t>(SMALLINT(), data);
  // Sparse: every 5th row.
  std::vector<int32_t> sparse;
  for (int i = 0; i < kSize; i += 5) {
    sparse.push_back(i);
  }
  testConversion<int32_t, int16_t>(SMALLINT(), data, sparse);
}

// Cross-domain upcast conversion with nulls.

TEST_F(GetFlatValuesTest, int32ToDoubleWithNulls) {
  auto reader = makeReader(DOUBLE());
  const std::vector<int32_t> data = {10, 20, 0, 40, 0};
  const std::vector<bool> nulls = {false, false, true, false, true};
  std::vector<int32_t> rowNums(data.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValuesWithNulls(data, nulls, rowNums);

  const RowSet rows(rowNums.data(), rowNums.size());
  VectorPtr result;
  reader->getFlatValues<int32_t, double>(rows, &result, DOUBLE(), true);

  auto* flat = result->as<FlatVector<double>>();
  ASSERT_NE(flat, nullptr);
  ASSERT_EQ(flat->size(), data.size());
  EXPECT_DOUBLE_EQ(flat->valueAt(0), 10.0);
  EXPECT_DOUBLE_EQ(flat->valueAt(1), 20.0);
  EXPECT_TRUE(flat->isNullAt(2));
  EXPECT_DOUBLE_EQ(flat->valueAt(3), 40.0);
  EXPECT_TRUE(flat->isNullAt(4));
}

// Sparse rows with nulls.

TEST_F(GetFlatValuesTest, int32ToDoubleSparseRowsWithNulls) {
  auto reader = makeReader(DOUBLE());
  const std::vector<int32_t> data = {10, 20, 0, 40, 0, 60};
  const std::vector<bool> nulls = {false, false, true, false, true, false};
  const std::vector<int32_t> allRows = {0, 1, 2, 3, 4, 5};
  reader->setupValuesWithNulls(data, nulls, allRows);

  // Select rows {0, 2, 3, 5} — includes null at index 2.
  const std::vector<int32_t> sparseRows = {0, 2, 3, 5};
  const RowSet rows(sparseRows.data(), sparseRows.size());
  VectorPtr result;
  reader->getFlatValues<int32_t, double>(rows, &result, DOUBLE(), true);

  auto* flat = result->as<FlatVector<double>>();
  ASSERT_NE(flat, nullptr);
  ASSERT_EQ(flat->size(), 4);
  EXPECT_DOUBLE_EQ(flat->valueAt(0), 10.0);
  EXPECT_TRUE(flat->isNullAt(1));
  EXPECT_DOUBLE_EQ(flat->valueAt(2), 40.0);
  EXPECT_DOUBLE_EQ(flat->valueAt(3), 60.0);
}

TEST_F(GetFlatValuesTest, int64ToInt128SparseRowsWithNulls) {
  auto reader = makeReader(DECIMAL(38, 0));
  const std::vector<int64_t> data = {100, 0, 300, 0, 500};
  const std::vector<bool> nulls = {false, true, false, true, false};
  const std::vector<int32_t> allRows = {0, 1, 2, 3, 4};
  reader->setupValuesWithNulls(data, nulls, allRows);

  const std::vector<int32_t> sparseRows = {0, 1, 4};
  const RowSet rows(sparseRows.data(), sparseRows.size());
  VectorPtr result;
  reader->getFlatValues<int64_t, int128_t>(rows, &result, DECIMAL(38, 0), true);

  auto* flat = result->as<FlatVector<int128_t>>();
  ASSERT_NE(flat, nullptr);
  ASSERT_EQ(flat->size(), 3);
  EXPECT_EQ(flat->valueAt(0), 100);
  EXPECT_TRUE(flat->isNullAt(1));
  EXPECT_EQ(flat->valueAt(2), 500);
}

// Tests for the reentrant DATE->TIMESTAMP extraction path.
// See convertDateToTimestampValues in SelectiveColumnReader.cpp: a single
// read of int32 days-since-epoch can be drained across multiple disjoint
// extraction calls. The function picks between two paths per call:
//   - bulk path (rows.size() >= numValues_/2): transmute all numValues_
//     days to Timestamps in place, then delegate to getFlatValues.
//   - small-subset path (rows.size() < numValues_/2): emit a freshly
//     allocated Timestamp vector and leave the source buffer untouched.
// The bulk path transitions valueSize_ from sizeof(int32_t) to
// sizeof(Timestamp) ("State B"); subsequent extractions are expected to
// route through getFlatValues<Timestamp, Timestamp> instead of calling
// convertDateToTimestampValues again.
class ConvertDateToTimestampTest : public ::testing::Test {
 protected:
  static constexpr int64_t kSecondsPerDay{86'400};

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("ConvertDateToTimestampTest");
    stats_ = std::make_unique<ColumnReaderStatistics>();
    params_ = std::make_unique<StubFormatParams>(*pool_, *stats_);
    scanSpec_ = std::make_unique<velox::common::ScanSpec>("test");
    scanSpec_->setProjectOut(true);
  }

  std::unique_ptr<TestColumnReader> makeReader() const {
    auto fileType = TypeWithId::create(DATE());
    return std::make_unique<TestColumnReader>(
        DATE(), std::move(fileType), *params_, *scanSpec_);
  }

  // Asserts that flat[i] == Timestamp(days[selectedRow[i]] * 86400, 0).
  static void expectTimestampsMatch(
      const VectorPtr& result,
      const std::vector<int32_t>& days,
      const std::vector<int32_t>& selectedRows) {
    auto* flat = result->as<FlatVector<Timestamp>>();
    ASSERT_NE(flat, nullptr);
    ASSERT_EQ(flat->size(), selectedRows.size());
    for (size_t i = 0; i < selectedRows.size(); ++i) {
      const auto src = days[selectedRows[i]];
      EXPECT_EQ(
          flat->valueAt(i),
          Timestamp(kSecondsPerDay * static_cast<int64_t>(src), 0))
          << "Mismatch at index " << i << " (source row " << selectedRows[i]
          << ")";
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<ColumnReaderStatistics> stats_;
  std::unique_ptr<StubFormatParams> params_;
  std::unique_ptr<velox::common::ScanSpec> scanSpec_;
};

// Single bulk call covering all rows. Triggers the in-place transmute, then
// the isFinal flag should lock further extractions out via mayGetValues_.
TEST_F(ConvertDateToTimestampTest, singleCallAllRows) {
  auto reader = makeReader();
  const std::vector<int32_t> days = {0, 1, 2, 3, 4, 5, 6, 7};
  std::vector<int32_t> rowNums(days.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValues(days, rowNums);

  const RowSet rows(rowNums.data(), rowNums.size());
  VectorPtr result;
  reader->convertDateToTimestampValues(rows, &result, /*isFinal=*/true);

  expectTimestampsMatch(result, days, rowNums);
  // Bulk path transmuted the buffer in place: valueSize_ flipped to
  // sizeof(Timestamp).
  EXPECT_EQ(reader->valueSize(), sizeof(Timestamp));
  EXPECT_EQ(reader->numValues(), 8);
  // isFinal=true released the mayGetValues_ lock.
  EXPECT_FALSE(reader->mayGetValues());
}

// Two disjoint small-subset calls, each well below the half-of-numValues_
// threshold. valueSize_ must stay at sizeof(int32_t) (State A is preserved).
TEST_F(ConvertDateToTimestampTest, singleCallSmallSubset) {
  auto reader = makeReader();
  std::vector<int32_t> days(20);
  std::iota(days.begin(), days.end(), 0);
  std::vector<int32_t> rowNums(days.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValues(days, rowNums);

  // First small-subset call (3 < 20/2).
  const std::vector<int32_t> firstSelected = {2, 5, 11};
  const RowSet firstRows(firstSelected.data(), firstSelected.size());
  VectorPtr firstResult;
  reader->convertDateToTimestampValues(firstRows, &firstResult);

  expectTimestampsMatch(firstResult, days, firstSelected);
  EXPECT_EQ(reader->valueSize(), sizeof(int32_t));
  EXPECT_TRUE(reader->mayGetValues());

  // Second small-subset call, disjoint from the first, marked final.
  const std::vector<int32_t> secondSelected = {1, 7, 19};
  const RowSet secondRows(secondSelected.data(), secondSelected.size());
  VectorPtr secondResult;
  reader->convertDateToTimestampValues(
      secondRows, &secondResult, /*isFinal=*/true);

  expectTimestampsMatch(secondResult, days, secondSelected);
  // Small-subset path never transitions to State B.
  EXPECT_EQ(reader->valueSize(), sizeof(int32_t));
  EXPECT_FALSE(reader->mayGetValues());
  EXPECT_EQ(reader->numValues(), 20);

  // The first result must still be intact after the second extraction.
  expectTimestampsMatch(firstResult, days, firstSelected);
}

// Three small-subset calls in sequence. Each result owns its own buffer and
// must remain readable after the others run. valueSize_ stays in State A.
TEST_F(ConvertDateToTimestampTest, twoSmallSubsetsThenSmallFinal) {
  auto reader = makeReader();
  std::vector<int32_t> days(30);
  std::iota(days.begin(), days.end(), 100);
  std::vector<int32_t> rowNums(days.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValues(days, rowNums);

  const std::vector<int32_t> selected1 = {0, 3};
  const std::vector<int32_t> selected2 = {7, 11, 12};
  const std::vector<int32_t> selected3 = {20, 29};
  VectorPtr r1, r2, r3;

  const RowSet rows1(selected1.data(), selected1.size());
  reader->convertDateToTimestampValues(rows1, &r1);
  EXPECT_EQ(reader->valueSize(), sizeof(int32_t));
  EXPECT_TRUE(reader->mayGetValues());

  const RowSet rows2(selected2.data(), selected2.size());
  reader->convertDateToTimestampValues(rows2, &r2);
  EXPECT_EQ(reader->valueSize(), sizeof(int32_t));
  EXPECT_TRUE(reader->mayGetValues());

  const RowSet rows3(selected3.data(), selected3.size());
  reader->convertDateToTimestampValues(rows3, &r3, /*isFinal=*/true);
  EXPECT_EQ(reader->valueSize(), sizeof(int32_t));
  EXPECT_FALSE(reader->mayGetValues());

  // All three results remain independently correct: no shared buffer.
  expectTimestampsMatch(r1, days, selected1);
  expectTimestampsMatch(r2, days, selected2);
  expectTimestampsMatch(r3, days, selected3);
}

// First a small-subset call, then a bulk call that triggers in-place
// transmute. The first result must continue to read correctly even though
// the source int32 buffer has been overwritten with 12-byte Timestamps.
TEST_F(ConvertDateToTimestampTest, smallSubsetThenBulk) {
  auto reader = makeReader();
  std::vector<int32_t> days(16);
  for (int i = 0; i < 16; ++i) {
    days[i] = i * 2;
  }
  std::vector<int32_t> rowNums(days.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValues(days, rowNums);

  // Small subset first (2 < 16/2 == 8). State A preserved.
  const std::vector<int32_t> smallSelected = {3, 9};
  const RowSet smallRows(smallSelected.data(), smallSelected.size());
  VectorPtr smallResult;
  reader->convertDateToTimestampValues(smallRows, &smallResult);
  EXPECT_EQ(reader->valueSize(), sizeof(int32_t));

  // Bulk call covering enough rows to clear the threshold (8 >= 16/2).
  const std::vector<int32_t> bulkSelected = {0, 1, 2, 3, 4, 5, 6, 7};
  const RowSet bulkRows(bulkSelected.data(), bulkSelected.size());
  VectorPtr bulkResult;
  reader->convertDateToTimestampValues(
      bulkRows, &bulkResult, /*isFinal=*/true);
  EXPECT_EQ(reader->valueSize(), sizeof(Timestamp));
  EXPECT_FALSE(reader->mayGetValues());

  // The small result must still be correct: it owns its own Timestamp
  // buffer, decoupled from the source bytes that the bulk call overwrote.
  expectTimestampsMatch(smallResult, days, smallSelected);
  expectTimestampsMatch(bulkResult, days, bulkSelected);
}

// Bulk call transitions to State B (valueSize_ == sizeof(Timestamp)), then
// follow-up extractions must use getFlatValues<Timestamp, Timestamp>
// directly: this is the contract that getIntValues encodes via its
// valueSize_ dispatch. The bulk call requests rows.size() == numValues_ so
// compactScalarValues early-returns and the full buffer remains available
// for subsequent subset extractions.
TEST_F(ConvertDateToTimestampTest, bulkThenStateBExtraction) {
  auto reader = makeReader();
  std::vector<int32_t> days(10);
  std::iota(days.begin(), days.end(), 50);
  std::vector<int32_t> rowNums(days.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValues(days, rowNums);

  // First call: bulk path. rows.size() == numValues_, so
  // getFlatValues -> compactScalarValues early-returns without compaction.
  const RowSet allRows(rowNums.data(), rowNums.size());
  VectorPtr firstResult;
  reader->convertDateToTimestampValues(allRows, &firstResult);
  expectTimestampsMatch(firstResult, days, rowNums);
  EXPECT_EQ(reader->valueSize(), sizeof(Timestamp));
  EXPECT_TRUE(reader->mayGetValues());

  // Second extraction: dispatch through getFlatValues<Timestamp, Timestamp>
  // because the buffer is now Timestamps (this is what getIntValues does).
  // Compacts in place: the buffer afterward only holds the requested rows.
  const std::vector<int32_t> secondSelected = {1, 4, 8};
  const RowSet secondRows(secondSelected.data(), secondSelected.size());
  VectorPtr secondResult;
  reader->getFlatValues<Timestamp, Timestamp>(
      secondRows, &secondResult, TIMESTAMP(), /*isFinal=*/false);
  expectTimestampsMatch(secondResult, days, secondSelected);
  EXPECT_TRUE(reader->mayGetValues());

  // Third extraction, marked final. Subset of what's still in the buffer.
  const std::vector<int32_t> thirdSelected = {1, 8};
  const RowSet thirdRows(thirdSelected.data(), thirdSelected.size());
  VectorPtr thirdResult;
  reader->getFlatValues<Timestamp, Timestamp>(
      thirdRows, &thirdResult, TIMESTAMP(), /*isFinal=*/true);
  expectTimestampsMatch(thirdResult, days, thirdSelected);
  EXPECT_FALSE(reader->mayGetValues());
}

// Two small-subset calls with nulls. Each call must produce correct null
// flags for its requested subset, and a prior result's nulls must not be
// clobbered by a subsequent call.
TEST_F(ConvertDateToTimestampTest, smallSubsetWithNulls) {
  auto reader = makeReader();
  // 10 source values with nulls at rows 1, 4, 7.
  const std::vector<int32_t> days = {10, 0, 20, 30, 0, 50, 60, 0, 80, 90};
  const std::vector<bool> nulls = {
      false,
      true,
      false,
      false,
      true,
      false,
      false,
      true,
      false,
      false};
  std::vector<int32_t> rowNums(days.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValuesWithNulls(days, nulls, rowNums);

  // First small subset: includes nulls at source positions 1 and 4.
  const std::vector<int32_t> firstSelected = {0, 1, 4};
  const RowSet firstRows(firstSelected.data(), firstSelected.size());
  VectorPtr firstResult;
  reader->convertDateToTimestampValues(firstRows, &firstResult);

  ASSERT_NE(firstResult, nullptr);
  auto* firstFlat = firstResult->as<FlatVector<Timestamp>>();
  ASSERT_NE(firstFlat, nullptr);
  ASSERT_EQ(firstFlat->size(), 3);
  EXPECT_FALSE(firstFlat->isNullAt(0));
  EXPECT_EQ(firstFlat->valueAt(0), Timestamp(kSecondsPerDay * 10, 0));
  EXPECT_TRUE(firstFlat->isNullAt(1));
  EXPECT_TRUE(firstFlat->isNullAt(2));

  // Second small subset: includes null at source position 7.
  const std::vector<int32_t> secondSelected = {3, 7, 9};
  const RowSet secondRows(secondSelected.data(), secondSelected.size());
  VectorPtr secondResult;
  reader->convertDateToTimestampValues(
      secondRows, &secondResult, /*isFinal=*/true);

  auto* secondFlat = secondResult->as<FlatVector<Timestamp>>();
  ASSERT_NE(secondFlat, nullptr);
  ASSERT_EQ(secondFlat->size(), 3);
  EXPECT_FALSE(secondFlat->isNullAt(0));
  EXPECT_EQ(secondFlat->valueAt(0), Timestamp(kSecondsPerDay * 30, 0));
  EXPECT_TRUE(secondFlat->isNullAt(1));
  EXPECT_FALSE(secondFlat->isNullAt(2));
  EXPECT_EQ(secondFlat->valueAt(2), Timestamp(kSecondsPerDay * 90, 0));

  // First result's null flags must still be correct after the second
  // extraction: nulls were allocated into a local buffer per call.
  EXPECT_FALSE(firstFlat->isNullAt(0));
  EXPECT_TRUE(firstFlat->isNullAt(1));
  EXPECT_TRUE(firstFlat->isNullAt(2));
  EXPECT_EQ(firstFlat->valueAt(0), Timestamp(kSecondsPerDay * 10, 0));
}

// allNull_ short-circuit: produces a ConstantVector<Timestamp> of the
// requested size; mayGetValues_ is left intact unless isFinal is set.
TEST_F(ConvertDateToTimestampTest, allNullsShortCircuit) {
  auto reader = makeReader();
  // numValues_ is required to be set for the function's invariants. Use
  // an arbitrary non-empty buffer; allNull_ short-circuits before reading.
  const std::vector<int32_t> days = {0, 0, 0, 0};
  std::vector<int32_t> rowNums(days.size());
  std::iota(rowNums.begin(), rowNums.end(), 0);
  reader->setupValues(days, rowNums);
  reader->setAllNull(true);

  // First call: not final. Expect ConstantVector<Timestamp> of size 3.
  const std::vector<int32_t> firstSelected = {0, 1, 2};
  const RowSet firstRows(firstSelected.data(), firstSelected.size());
  VectorPtr firstResult;
  reader->convertDateToTimestampValues(firstRows, &firstResult);

  ASSERT_NE(firstResult, nullptr);
  auto* firstConst = firstResult->as<ConstantVector<Timestamp>>();
  ASSERT_NE(firstConst, nullptr);
  EXPECT_EQ(firstConst->size(), 3);
  EXPECT_TRUE(firstConst->isNullAt(0));
  EXPECT_TRUE(reader->mayGetValues());

  // Second call: final. Expect another ConstantVector<Timestamp>; lock flips.
  const std::vector<int32_t> secondSelected = {0, 1};
  const RowSet secondRows(secondSelected.data(), secondSelected.size());
  VectorPtr secondResult;
  reader->convertDateToTimestampValues(
      secondRows, &secondResult, /*isFinal=*/true);

  auto* secondConst = secondResult->as<ConstantVector<Timestamp>>();
  ASSERT_NE(secondConst, nullptr);
  EXPECT_EQ(secondConst->size(), 2);
  EXPECT_TRUE(secondConst->isNullAt(0));
  EXPECT_FALSE(reader->mayGetValues());
}

} // namespace
} // namespace facebook::velox::dwio::common
