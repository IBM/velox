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

#include "velox/dwio/parquet/reader/ParquetData.h"

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/parquet/common/ParquetRuntimeStats.h"
#include "velox/dwio/parquet/reader/ParquetStatsContext.h"

namespace facebook::velox::parquet {

std::unique_ptr<dwio::common::FormatData> ParquetParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const common::ScanSpec& /*scanSpec*/) {
  return std::make_unique<ParquetData>(
      type,
      metaData_,
      pool(),
      columnStats(type->id(), type->type()->kind()),
      sessionTimezone_);
}

void ParquetData::filterRowGroups(
    const common::ScanSpec& scanSpec,
    uint64_t /*rowsPerRowGroup*/,
    const dwio::common::StatsContext& writerContext,
    FilterRowGroupsResult& result) {
  auto parquetStatsContext =
      reinterpret_cast<const ParquetStatsContext*>(&writerContext);
  if (type_->parquetType_.has_value() &&
      parquetStatsContext->shouldIgnoreStatistics(
          type_->parquetType_.value())) {
    return;
  }
  result.totalCount =
      std::max<int>(result.totalCount, fileMetaDataPtr_.numRowGroups());
  auto nwords = bits::nwords(result.totalCount);
  if (result.filterResult.size() < nwords) {
    result.filterResult.resize(nwords);
  }
  auto metadataFiltersStartIndex = result.metadataFilterResults.size();
  for (int i = 0; i < scanSpec.numMetadataFilters(); ++i) {
    result.metadataFilterResults.emplace_back(
        scanSpec.metadataFilterNodeAt(i), std::vector<uint64_t>(nwords));
  }
  if (scanSpec.filter() || scanSpec.numMetadataFilters() > 0) {
    for (auto i = 0; i < fileMetaDataPtr_.numRowGroups(); ++i) {
      // Already excluded by another column or by the caller (e.g. row group
      // outside the split range, empty row group). Skip statistics build and
      // testFilter. The MetadataFilter::eval call ORs into filterResult, so
      // leaving the per-leaf metadata bits at 0 here is harmless: filterResult
      // already has the bit set.
      if (bits::isBitSet(result.filterResult.data(), i)) {
        continue;
      }
      if (scanSpec.filter() && !rowGroupMatches(i, scanSpec.filter())) {
        bits::setBit(result.filterResult.data(), i);
        continue;
      }
      for (int j = 0; j < scanSpec.numMetadataFilters(); ++j) {
        auto* metadataFilter = scanSpec.metadataFilterAt(j);
        if (!rowGroupMatches(i, metadataFilter)) {
          bits::setBit(
              result.metadataFilterResults[metadataFiltersStartIndex + j]
                  .second.data(),
              i);
        }
      }
    }
  }
}

bool ParquetData::rowGroupMatches(
    uint32_t rowGroupId,
    const common::Filter* filter) {
  auto column = type_->column();
  auto type = type_->type();
  auto rowGroup = fileMetaDataPtr_.rowGroup(rowGroupId);
  assert(rowGroup.numColumns() != 0);

  if (!filter) {
    return true;
  }

  auto columnChunk = rowGroup.columnChunk(column);
  if (columnChunk.hasStatistics()) {
    auto columnStats = columnChunk.getColumnStatistics(
        type_->type(),
        rowGroup.numRows(),
        type_->convertedType_,
        type_->logicalType_);
    return testFilter(filter, columnStats.get(), rowGroup.numRows(), type);
  }
  return true;
}

void ParquetData::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto chunk = fileMetaDataPtr_.rowGroup(index).columnChunk(type_->column());
  streams_.resize(fileMetaDataPtr_.numRowGroups());
  VELOX_CHECK(
      chunk.hasMetadata(),
      "ColumnMetaData does not exist for schema Id ",
      type_->column());
  ;

  uint64_t chunkReadOffset = chunk.dataPageOffset();
  if (chunk.hasDictionaryPageOffset() && chunk.dictionaryPageOffset() >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunkReadOffset = chunk.dictionaryPageOffset();
  }

  auto id = dwio::common::StreamIdentifier(type_->column());
  streams_[index] = input.enqueue({chunkReadOffset, chunk.readSize()}, &id);
}

dwio::common::PositionProvider ParquetData::seekToRowGroup(int64_t index) {
  static std::vector<uint64_t> empty;
  VELOX_CHECK_LT(index, streams_.size());
  VELOX_CHECK(streams_[index], "Stream not enqueued for column");
  auto metadata = fileMetaDataPtr_.rowGroup(index).columnChunk(type_->column());
  reader_ = std::make_unique<PageReader>(
      std::move(streams_[index]),
      pool_,
      type_,
      metadata.compression(),
      metadata.totalCompressedSize(),
      stats_,
      sessionTimezone_);
  return dwio::common::PositionProvider(empty);
}

std::pair<int64_t, int64_t> ParquetData::getRowGroupRegion(
    uint32_t index) const {
  auto rowGroup = fileMetaDataPtr_.rowGroup(index);

  VELOX_CHECK_GT(rowGroup.numColumns(), 0);
  auto fileOffset = (rowGroup.hasFileOffset() && rowGroup.fileOffset() != 0)
      ? rowGroup.fileOffset()
      : rowGroup.columnChunk(0).hasDictionaryPageOffset()
      ? rowGroup.columnChunk(0).dictionaryPageOffset()
      : rowGroup.columnChunk(0).dataPageOffset();
  VELOX_CHECK_GT(fileOffset, 0);

  auto length = rowGroup.hasTotalCompressedSize()
      ? rowGroup.totalCompressedSize()
      : rowGroup.totalByteSize();

  return {fileOffset, length};
}

std::optional<velox::common::Region> ParquetData::dictionaryRegion(
    uint32_t rowGroupId,
    const common::Filter& filter) const {
  // Reads only BYTE_ARRAY dictionaries. FIXED_LEN_BYTE_ARRAY can also be read
  // as VARCHAR or VARBINARY.
  const auto kind = type_->type()->kind();
  if (maxRepeat_ > 0 ||
      (kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) ||
      type_->parquetType_ != thrift::Type::BYTE_ARRAY) {
    return std::nullopt;
  }
  auto columnChunk =
      fileMetaDataPtr_.rowGroup(rowGroupId).columnChunk(type_->column());
  if (!columnChunk.hasMetadata() || !columnChunk.hasDictionaryPageOffset() ||
      !columnChunk.hasOnlyDictionaryEncodedDataPages()) {
    return std::nullopt;
  }
  // The dictionary does not hold nulls, so it decides only if nulls cannot
  // pass. An unknown null count may hide nulls that pass.
  if (maxDefine_ > 0 && filter.testNull() &&
      columnChunk.nullCount().value_or(1) > 0) {
    return std::nullopt;
  }
  // The dictionary page directly precedes the data pages. An offset inside the
  // 4-byte "PAR1" magic is invalid, e.g. 0 from writers that have no
  // dictionary page.
  const auto offset = columnChunk.dictionaryPageOffset();
  const auto length = columnChunk.dataPageOffset() - offset;
  if (offset < 4 || length <= 0) {
    return std::nullopt;
  }
  return velox::common::Region{
      static_cast<uint64_t>(offset), static_cast<uint64_t>(length)};
}

bool ParquetData::dictionaryMatches(
    uint32_t rowGroupId,
    const common::Filter& filter,
    std::unique_ptr<dwio::common::SeekableInputStream> stream) const {
  auto columnChunk =
      fileMetaDataPtr_.rowGroup(rowGroupId).columnChunk(type_->column());
  const auto length =
      columnChunk.dataPageOffset() - columnChunk.dictionaryPageOffset();
  PageReader reader(
      std::move(stream),
      pool_,
      type_,
      columnChunk.compression(),
      length,
      stats_,
      sessionTimezone_);
  if (!reader.readDictionaryPage()) {
    return true;
  }
  const auto* values =
      reader.dictionaryValues(type_->type())->asFlatVector<StringView>();
  for (auto i = 0; i < values->size(); ++i) {
    if (filter.testStringView(values->valueAt(i))) {
      return true;
    }
  }
  stats_.accumulateStat(
      ParquetRuntimeStats::kNumRowGroupsSkippedByDictionaryMetric, 1);
  return false;
}

std::optional<std::pair<velox::common::Region, velox::common::Region>>
ParquetData::pageIndexRegions(uint32_t rowGroupId) const {
  auto columnChunk =
      fileMetaDataPtr_.rowGroup(rowGroupId).columnChunk(type_->column());
  if (!columnChunk.hasMetadata() || !columnChunk.hasColumnIndex() ||
      !columnChunk.hasOffsetIndex()) {
    return std::nullopt;
  }
  const auto [columnIndexOffset, columnIndexLength] =
      columnChunk.columnIndexRegion();
  const auto [offsetIndexOffset, offsetIndexLength] =
      columnChunk.offsetIndexRegion();
  if (columnIndexLength <= 0 || offsetIndexLength <= 0) {
    return std::nullopt;
  }
  return std::make_pair(
      velox::common::Region{
          static_cast<uint64_t>(columnIndexOffset),
          static_cast<uint64_t>(columnIndexLength)},
      velox::common::Region{
          static_cast<uint64_t>(offsetIndexOffset),
          static_cast<uint64_t>(offsetIndexLength)});
}

std::optional<std::vector<RowRange>> ParquetData::pageIndexRowRanges(
    uint32_t rowGroupId,
    const common::Filter& filter,
    std::string_view serializedColumnIndex,
    std::string_view serializedOffsetIndex) const {
  thrift::ColumnIndex columnIndex;
  thrift::OffsetIndex offsetIndex;
  try {
    thrift::deserialize(&columnIndex, serializedColumnIndex);
    thrift::deserialize(&offsetIndex, serializedOffsetIndex);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Ignoring a malformed page index: " << e.what();
    return std::nullopt;
  }
  const auto& pageLocations = *offsetIndex.page_locations();
  const auto& nullPages = *columnIndex.null_pages();
  auto& minValues = *columnIndex.min_values();
  auto& maxValues = *columnIndex.max_values();
  const auto& nullCounts = columnIndex.null_counts();
  const auto numPages = pageLocations.size();
  if (nullPages.size() != numPages || minValues.size() != numPages ||
      maxValues.size() != numPages ||
      (nullCounts.has_value() && nullCounts->size() != numPages)) {
    return std::nullopt;
  }

  auto rowGroup = fileMetaDataPtr_.rowGroup(rowGroupId);
  const auto numRows = rowGroup.numRows();
  // The pages must cover the row group from its first row, in order.
  for (size_t page = 0; page < numPages; ++page) {
    const int64_t firstRow = *pageLocations[page].first_row_index();
    const int64_t previousFirstRow =
        page == 0 ? -1 : *pageLocations[page - 1].first_row_index();
    if ((page == 0 && firstRow != 0) || firstRow <= previousFirstRow ||
        firstRow >= numRows) {
      return std::nullopt;
    }
  }
  auto columnChunk = rowGroup.columnChunk(type_->column());
  // Returns true if 'filter' may pass a value of the 'page'th page, which has
  // 'numPageRows' rows, testing it the same way as a row group.
  const auto pageMayMatch = [&](size_t page, int64_t numPageRows) {
    if (nullPages[page]) {
      return filter.testNull();
    }
    thrift::Statistics pageStats;
    pageStats.min_value() = std::move(minValues[page]);
    pageStats.max_value() = std::move(maxValues[page]);
    if (nullCounts.has_value()) {
      pageStats.null_count() = (*nullCounts)[page];
    }
    const auto stats = columnChunk.getPageStatistics(
        pageStats,
        type_->type(),
        numPageRows,
        type_->convertedType_,
        type_->logicalType_);
    return testFilter(&filter, stats.get(), numPageRows, type_->type());
  };

  std::vector<RowRange> ranges;
  for (size_t page = 0; page < numPages; ++page) {
    const int64_t begin = *pageLocations[page].first_row_index();
    const int64_t end = page + 1 < numPages
        ? *pageLocations[page + 1].first_row_index()
        : numRows;
    if (!pageMayMatch(page, end - begin)) {
      continue;
    }
    // Merges the ranges of consecutive pages.
    if (!ranges.empty() && ranges.back().end == begin) {
      ranges.back().end = end;
    } else {
      ranges.push_back({begin, end});
    }
  }
  return ranges;
}

} // namespace facebook::velox::parquet
