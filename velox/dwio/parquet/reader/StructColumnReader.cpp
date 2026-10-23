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

#include "velox/dwio/parquet/reader/StructColumnReader.h"

#include <algorithm>
#include <optional>
#include <tuple>

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/StreamUtil.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetData.h"
#include "velox/dwio/parquet/reader/ParquetStatsContext.h"
#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"

namespace facebook::velox::common {
class ScanSpec;
}

namespace facebook::velox::parquet {
namespace {

struct PhysicalLeafCost {
  const ParquetTypeWithId* type;
  bool hasCompleteMetadata{true};
  uint64_t readBytes{0};
  uint64_t uncompressedBytes{0};
  uint64_t numValues{0};

  bool operator<(const PhysicalLeafCost& other) const {
    if (hasCompleteMetadata != other.hasCompleteMetadata) {
      // Prefer a leaf with metadata for every non-empty row group. Partial
      // metadata can underestimate its cost and may make the leaf unreadable.
      return hasCompleteMetadata;
    }
    if (!hasCompleteMetadata) {
      // Neither cost is reliable. Use schema order as a deterministic fallback.
      return type->column() < other.type->column();
    }
    // Minimize bytes fetched first, then bytes decompressed and levels decoded.
    // Use schema order to make costs deterministic.
    return std::tuple{readBytes, uncompressedBytes, numValues, type->column()} <
        std::tuple{
            other.readBytes,
            other.uncompressedBytes,
            other.numValues,
            other.type->column()};
  }
};

PhysicalLeafCost physicalLeafCost(
    const ParquetTypeWithId& type,
    const FileMetaDataPtr& fileMetaData) {
  PhysicalLeafCost cost{.type = &type};
  // The source reader is fixed before row-group filtering. Use all non-empty
  // row groups as a whole-file cost estimate; an individual split may have a
  // different cheapest leaf for its subset of row groups.
  for (auto i = 0; i < fileMetaData.numRowGroups(); ++i) {
    auto rowGroup = fileMetaData.rowGroup(i);
    if (rowGroup.numRows() == 0) {
      continue;
    }
    if (type.column() >= static_cast<uint32_t>(rowGroup.numColumns())) {
      cost.hasCompleteMetadata = false;
      return cost;
    }
    auto chunk = rowGroup.columnChunk(type.column());
    if (!chunk.hasMetadata()) {
      cost.hasCompleteMetadata = false;
      return cost;
    }
    const auto readSize = chunk.readSize();
    if (readSize == 0) {
      // Size metadata is optional. A non-empty row group cannot have a
      // zero-byte column chunk, so zero means the read cost is unknown.
      cost.hasCompleteMetadata = false;
      return cost;
    }
    cost.readBytes += readSize;
    cost.uncompressedBytes +=
        static_cast<uint64_t>(chunk.totalUncompressedSize());
    cost.numValues += static_cast<uint64_t>(chunk.numValues());
  }
  return cost;
}

std::optional<PhysicalLeafCost> findCheapestPhysicalLeafImpl(
    const ParquetTypeWithId& type,
    const FileMetaDataPtr& fileMetaData) {
  if (type.getChildren().empty()) {
    if (type.isLeaf()) {
      return physicalLeafCost(type, fileMetaData);
    }
    return std::nullopt;
  }

  std::optional<PhysicalLeafCost> best;
  for (auto i = 0; i < type.getChildren().size(); ++i) {
    auto candidate =
        findCheapestPhysicalLeafImpl(type.parquetChildAt(i), fileMetaData);
    if (candidate && (!best || *candidate < *best)) {
      best = std::move(candidate);
    }
  }
  return best;
}

const ParquetTypeWithId& findCheapestPhysicalLeaf(
    const ParquetTypeWithId& type,
    const FileMetaDataPtr& fileMetaData) {
  auto best = findCheapestPhysicalLeafImpl(type, fileMetaData);
  VELOX_CHECK(
      best.has_value(),
      "Cannot source repetition/definition levels for nested struct: {}",
      type.fullName());
  return *best->type;
}

LevelMode repDefSourceLevelMode(
    const ParquetTypeWithId& structType,
    const ParquetTypeWithId& sourceType) {
  const auto* node = &sourceType;
  while (node != &structType) {
    if (node->type()->kind() == TypeKind::ARRAY ||
        node->type()->kind() == TypeKind::MAP) {
      return LevelMode::kStructOverLists;
    }
    node = node->parquetParent();
  }
  return LevelMode::kNulls;
}

const ParquetTypeWithId& repDefSourceType(
    const dwio::common::SelectiveColumnReader& reader) {
  const auto* source = &reader;
  while (source->fileType().type()->kind() == TypeKind::ROW) {
    source =
        static_cast<const StructColumnReader*>(source)->repDefSourceReader();
  }
  return *reinterpret_cast<const ParquetTypeWithId*>(&source->fileType());
}

} // namespace

struct StructColumnReader::SyntheticRepDefSource {
  // Members are destroyed in reverse declaration order. Keep the ScanSpec
  // alive until after the reader that references it is destroyed.
  std::unique_ptr<common::ScanSpec> scanSpec;

  // Reads repetition and definition levels without producing values.
  std::unique_ptr<dwio::common::SelectiveColumnReader> reader;
};

StructColumnReader::~StructColumnReader() = default;

StructColumnReader::StructColumnReader(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveStructColumnReader(
          columnReaderOptions,
          requestedType,
          fileType,
          params,
          scanSpec),
      dictionaryFilterInput_(params.dictionaryFilterInput()) {
  const auto stableChildren = scanSpec_->stableChildren();
  const auto& childSpecs = *stableChildren;
  for (auto i = 0; i < childSpecs.size(); ++i) {
    const auto& childSpec = childSpecs[i];
    if (childSpec->isConstant() || isChildMissing(*childSpec)) {
      childSpec->setSubscript(kConstantChildSpecSubscript);
      continue;
    }
    if (!childSpecs[i]->readFromFile()) {
      continue;
    }
    auto childFileType = fileType_->childByName(childSpec->fieldName());
    auto childRequestedType =
        requestedType_->asRow().findChild(childSpec->fieldName());
    addChild(
        ParquetColumnReader::build(
            columnReaderOptions,
            childRequestedType,
            childFileType,
            params,
            *childSpec));

    childSpecs[i]->setSubscript(children_.size() - 1);
  }
  applyMissingFieldPolicy(
      columnReaderOptions, params.nullStructIfAllFieldsMissing());
  ensureSyntheticRepDefSource(columnReaderOptions, params);
  auto type = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get());
  if (type->parent()) {
    type->makeLevelInfo(levelInfo_);
    repDefSourceReader_ = findBestLeaf();
    levelMode_ =
        repDefSourceLevelMode(*type, repDefSourceType(*repDefSourceReader_));
  }
  VELOX_DCHECK_EQ(type->parent() == nullptr, repDefSourceReader_ == nullptr);
}

void StructColumnReader::applyMissingFieldPolicy(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    bool nullStructIfAllFieldsMissing) {
  const bool useColumnNames = columnReaderOptions.columnMappingMode_ ==
      dwio::common::ColumnMappingMode::kName;
  if (!nullStructIfAllFieldsMissing || !useColumnNames) {
    return;
  }

  const auto stableChildren = scanSpec_->stableChildren();
  const auto& childSpecs = *stableChildren;
  if (childSpecs.empty()) {
    nullStructForMissingFields_ = true;
    return;
  }

  if (std::all_of(
          childSpecs.begin(), childSpecs.end(), [&](const auto& childSpec) {
            return childSpec->columnType() ==
                common::ScanSpec::ColumnType::kRegular &&
                isChildMissing(*childSpec);
          })) {
    nullStructForMissingFields_ = true;
  }
}

void StructColumnReader::ensureSyntheticRepDefSource(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    ParquetParams& params) {
  auto type = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get());
  if (!type->parent() || !children_.empty()) {
    return;
  }

  const auto* leafType =
      &findCheapestPhysicalLeaf(*type, params.fileMetaData());
  std::shared_ptr<const dwio::common::TypeWithId> leafFileType(
      fileType_, leafType);

  // Struct nullness can be derived from any leaf under the struct. Keep the
  // least expensive physical leaf solely as the repetition/definition source.
  auto repDefSource = std::make_unique<SyntheticRepDefSource>();
  repDefSource->scanSpec = std::make_unique<common::ScanSpec>(leafType->name_);
  repDefSource->scanSpec->setProjectOut(false);
  repDefSource->reader = ParquetColumnReader::build(
      columnReaderOptions,
      leafFileType->type(),
      leafFileType,
      params,
      *repDefSource->scanSpec);
  syntheticRepDefSource_ = std::move(repDefSource);
}

dwio::common::SelectiveColumnReader* FOLLY_NONNULL
StructColumnReader::findBestLeaf() {
  if (children_.empty()) {
    return syntheticRepDefSource_->reader.get();
  }

  SelectiveColumnReader* best = nullptr;
  for (auto i = 0; i < children_.size(); ++i) {
    auto child = children_[i];
    auto kind = child->fileType().type()->kind();
    // Complex type child repdefs must be read in any case.
    if (kind == TypeKind::ROW || kind == TypeKind::ARRAY) {
      return child;
    }
    if (!best) {
      best = child;
    } else if (best->scanSpec()->filter() && !child->scanSpec()->filter()) {
      continue;
    } else if (!best->scanSpec()->filter() && child->scanSpec()->filter()) {
      best = child;
      continue;
    } else if (kind < best->fileType().type()->kind()) {
      best = child;
    }
  }
  return best;
}

void StructColumnReader::read(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* /*incomingNulls*/) {
  prepareRepDefsAndOffset(*this, offset, rows);
  SelectiveStructColumnReader::read(offset, rows, nullptr);
}

std::shared_ptr<dwio::common::BufferedInput> StructColumnReader::loadRowGroup(
    uint32_t index,
    const std::shared_ptr<dwio::common::BufferedInput>& input) {
  if (isRowGroupBuffered(index, *input)) {
    enqueueRowGroup(index, *input);
    return input;
  }
  auto newInput = input->clone();
  enqueueRowGroup(index, *newInput);
  newInput->load(dwio::common::LogType::STRIPE);
  return newInput;
}

bool StructColumnReader::isRowGroupBuffered(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto [offset, length] =
      formatData().as<ParquetData>().getRowGroupRegion(index);
  return input.isBuffered(offset, length);
}

void StructColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueRowGroupRecursive(*this, index, input);
}

void StructColumnReader::seekToRowGroup(int64_t index) {
  SelectiveStructColumnReader::seekToRowGroup(index);
  BufferPtr noBuffer;
  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  readOffset_ = 0;
  for (auto& child : children_) {
    child->seekToRowGroup(index);
  }
  // Keep the rep/def source in sync when switching row groups.
  if (syntheticRepDefSource_) {
    syntheticRepDefSource_->reader->seekToRowGroup(index);
  }
}

void StructColumnReader::seekToEndOfPresetNulls() {
  auto numUnread = formatData_->as<ParquetData>().presetNullsLeft();
  for (auto i = 0; i < children_.size(); ++i) {
    auto child = children_[i];
    if (!child) {
      continue;
    }

    if (child->fileType().type()->kind() != TypeKind::ROW) {
      child->seekTo(readOffset_ + numUnread, false);
    } else if (child->fileType().type()->kind() == TypeKind::ROW) {
      reinterpret_cast<StructColumnReader*>(child)->seekToEndOfPresetNulls();
    }
  }
  readOffset_ += numUnread;
  formatData_->as<ParquetData>().skipNulls(numUnread, false);
}

void StructColumnReader::setNullsFromRepDefs(PageReader& pageReader) {
  if (levelInfo_.defLevel == 0 && !nullStructForMissingFields_) {
    return;
  }
  auto repDefRange = pageReader.repDefRange();
  int32_t numRepDefs = repDefRange.second - repDefRange.first;
  dwio::common::ensureCapacity<uint64_t>(
      nullsInReadRange_, bits::nwords(numRepDefs), pool_);
  // A required struct has no definition levels. In that case, each entry in
  // the decoded range represents one struct directly.
  auto numStructs = numRepDefs;
  if (levelInfo_.defLevel != 0) {
    numStructs = pageReader.getLengthsAndNulls(
        levelMode_,
        levelInfo_,
        repDefRange.first,
        repDefRange.second,
        numRepDefs,
        nullptr,
        nullsInReadRange()->asMutable<uint64_t>(),
        0);
  }
  // Repeated parents still need rep/def levels to determine the number of
  // structs. Preserve that count, but mark every struct null when the
  // missing-field policy applies to this reader.
  if (nullStructForMissingFields_) {
    bits::fillBits(
        nullsInReadRange()->asMutable<uint64_t>(), 0, numStructs, bits::kNull);
  }
  formatData_->as<ParquetData>().setNulls(nullsInReadRange(), numStructs);
}

namespace {
// Returns the rows in both 'left' and 'right'. Each is sorted and disjoint.
std::vector<RowRange> intersectRowRanges(
    const std::vector<RowRange>& left,
    const std::vector<RowRange>& right) {
  std::vector<RowRange> result;
  size_t leftIndex{0};
  size_t rightIndex{0};
  while (leftIndex < left.size() && rightIndex < right.size()) {
    const auto begin = std::max(left[leftIndex].begin, right[rightIndex].begin);
    const auto end = std::min(left[leftIndex].end, right[rightIndex].end);
    if (begin < end) {
      result.push_back({begin, end});
    }
    if (left[leftIndex].end < right[rightIndex].end) {
      ++leftIndex;
    } else {
      ++rightIndex;
    }
  }
  return result;
}

// Reads the next 'length' bytes of 'stream'.
std::string readFully(
    dwio::common::SeekableInputStream& stream,
    size_t length) {
  std::string bytes(length, '\0');
  const char* bufferStart{nullptr};
  const char* bufferEnd{nullptr};
  dwio::common::readBytes(
      bytes.size(), &stream, bytes.data(), bufferStart, bufferEnd);
  return bytes;
}

// Returns true if 'child' is a top-level leaf with a filter whose statistics
// 'statsContext' trusts.
bool hasFilterOnTrustedStats(
    const dwio::common::SelectiveColumnReader& child,
    const ParquetStatsContext& statsContext) {
  if (child.scanSpec()->filter() == nullptr) {
    return false;
  }
  const auto& fileType =
      static_cast<const ParquetTypeWithId&>(child.fileType());
  return fileType.isLeaf() && fileType.maxRepeat_ == 0 &&
      !(fileType.parquetType_.has_value() &&
        statsContext.shouldIgnoreStatistics(fileType.parquetType_.value()));
}

// Returns the children of the root struct that are top-level leaves with a
// filter whose statistics 'statsContext' trusts. Their filters are tested
// against dictionaries and page indexes the same way as against statistics.
std::vector<const dwio::common::SelectiveColumnReader*>
filteredLeavesWithTrustedStats(
    const std::vector<dwio::common::SelectiveColumnReader*>& children,
    const ParquetStatsContext& statsContext) {
  std::vector<const dwio::common::SelectiveColumnReader*> leaves;
  for (const auto* child : children) {
    if (child != nullptr && hasFilterOnTrustedStats(*child, statsContext)) {
      leaves.push_back(child);
    }
  }
  return leaves;
}
} // namespace

void StructColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  for (const auto& child : children_) {
    child->filterRowGroups(rowGroupSize, context, result);
  }
  // Only the root struct reads dictionaries, after its children have tested
  // the statistics of all columns, so that no dictionary is read for a row
  // group that statistics skip.
  const bool isRoot = repDefSourceReader_ == nullptr;
  if (isRoot && dictionaryFilterInput_ != nullptr) {
    filterRowGroupsByDictionary(
        static_cast<const ParquetStatsContext&>(context), result);
  }
}

std::optional<std::vector<RowRange>> StructColumnReader::pageIndexRowRanges(
    uint32_t rowGroupId,
    dwio::common::BufferedInput& input,
    const ParquetStatsContext& statsContext) const {
  // Enqueues the page indexes of all filtered columns so that they are loaded
  // together. Writers usually store the page indexes of all columns next to
  // each other, so the input coalesces them into few reads.
  struct PageIndexRead {
    const dwio::common::SelectiveColumnReader* leaf;
    velox::common::Region columnIndex;
    velox::common::Region offsetIndex;
    std::unique_ptr<dwio::common::SeekableInputStream> columnIndexStream;
    std::unique_ptr<dwio::common::SeekableInputStream> offsetIndexStream;
  };
  auto pageIndexInput = input.clone();
  std::vector<PageIndexRead> reads;
  for (const auto* leaf :
       filteredLeavesWithTrustedStats(children_, statsContext)) {
    const auto regions =
        leaf->formatData().as<ParquetData>().pageIndexRegions(rowGroupId);
    if (!regions.has_value()) {
      continue;
    }
    const auto& [columnIndex, offsetIndex] = regions.value();
    reads.push_back(
        {leaf,
         columnIndex,
         offsetIndex,
         pageIndexInput->enqueue(columnIndex),
         pageIndexInput->enqueue(offsetIndex)});
  }
  if (reads.empty()) {
    return std::nullopt;
  }
  pageIndexInput->load(dwio::common::LogType::STREAM);

  std::optional<std::vector<RowRange>> result;
  for (const auto& read : reads) {
    auto ranges = read.leaf->formatData().as<ParquetData>().pageIndexRowRanges(
        rowGroupId,
        *read.leaf->scanSpec()->filter(),
        readFully(*read.columnIndexStream, read.columnIndex.length),
        readFully(*read.offsetIndexStream, read.offsetIndex.length));
    if (!ranges.has_value()) {
      continue;
    }
    result = result.has_value()
        ? intersectRowRanges(result.value(), ranges.value())
        : std::move(ranges);
  }
  return result;
}

void StructColumnReader::filterRowGroupsByDictionary(
    const ParquetStatsContext& statsContext,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  struct Dictionary {
    const dwio::common::SelectiveColumnReader* reader;
    uint32_t rowGroup;
    std::unique_ptr<dwio::common::SeekableInputStream> stream;
  };
  // Caps the bytes of dictionary pages held in memory at once.
  constexpr uint64_t kMaxBytesPerLoad = 64 << 20;
  const auto leaves = filteredLeavesWithTrustedStats(children_, statsContext);
  std::unique_ptr<dwio::common::BufferedInput> input;
  std::vector<Dictionary> dictionaries;
  uint64_t numBytes{0};
  // Loads the enqueued dictionary pages together and skips the row groups
  // whose dictionary has no passing value.
  const auto testDictionaries = [&]() {
    if (dictionaries.empty()) {
      return;
    }
    input->load(dwio::common::LogType::STREAM);
    for (auto& dictionary : dictionaries) {
      // The dictionary of another column may already have skipped the row
      // group.
      if (bits::isBitSet(result.filterResult.data(), dictionary.rowGroup)) {
        continue;
      }
      if (!dictionary.reader->formatData().as<ParquetData>().dictionaryMatches(
              dictionary.rowGroup,
              *dictionary.reader->scanSpec()->filter(),
              std::move(dictionary.stream))) {
        bits::setBit(result.filterResult.data(), dictionary.rowGroup);
      }
    }
    dictionaries.clear();
    input.reset();
    numBytes = 0;
  };
  for (uint32_t rowGroup = 0; rowGroup < result.totalCount; ++rowGroup) {
    if (bits::isBitSet(result.filterResult.data(), rowGroup)) {
      continue;
    }
    for (const auto* leaf : leaves) {
      const auto region = leaf->formatData().as<ParquetData>().dictionaryRegion(
          rowGroup, *leaf->scanSpec()->filter());
      if (!region.has_value()) {
        continue;
      }
      if (input == nullptr) {
        input = dictionaryFilterInput_->clone();
      }
      dictionaries.push_back({leaf, rowGroup, input->enqueue(region.value())});
      numBytes += region->length;
      if (numBytes >= kMaxBytesPerLoad) {
        testDictionaries();
      }
    }
  }
  testDictionaries();
}

} // namespace facebook::velox::parquet
