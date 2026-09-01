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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"
#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"

namespace facebook::velox::common {
class ScanSpec;
}

namespace facebook::velox::parquet {
namespace {

const ParquetTypeWithId* findFirstPhysicalLeafImpl(
    const ParquetTypeWithId& type) {
  if (type.getChildren().empty()) {
    return type.isLeaf() ? &type : nullptr;
  }

  for (auto i = 0; i < type.getChildren().size(); ++i) {
    if (const auto* leaf = findFirstPhysicalLeafImpl(type.parquetChildAt(i))) {
      return leaf;
    }
  }
  return nullptr;
}

const ParquetTypeWithId& findFirstPhysicalLeaf(const ParquetTypeWithId& type) {
  const auto* leaf = findFirstPhysicalLeafImpl(type);
  VELOX_CHECK_NOT_NULL(
      leaf,
      "Cannot source repetition/definition levels for nested struct: {}",
      type.fullName());
  return *leaf;
}

LevelMode repDefSourceLevelMode(
    const ParquetTypeWithId& structType,
    const ParquetTypeWithId& sourceType) {
  const auto* node = &sourceType;
  while (node != &structType) {
    VELOX_CHECK_NOT_NULL(
        node,
        "Cannot source repetition/definition levels for nested struct: {}",
        structType.fullName());
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
    const auto* structReader = dynamic_cast<const StructColumnReader*>(source);
    VELOX_CHECK_NOT_NULL(
        structReader,
        "Cannot source repetition/definition levels for nested struct: {}",
        source->fileType().fullName());
    source = structReader->repDefSourceReader();
    VELOX_CHECK_NOT_NULL(
        source,
        "Cannot source repetition/definition levels for nested struct: {}",
        structReader->fileType().fullName());
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
          scanSpec) {
  auto& childSpecs = scanSpec_->stableChildren();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto childSpec = childSpecs[i];
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
}

void StructColumnReader::applyMissingFieldPolicy(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    bool nullStructIfAllFieldsMissing) {
  if (!nullStructIfAllFieldsMissing) {
    return;
  }

  auto& childSpecs = scanSpec_->stableChildren();
  if (childSpecs.empty()) {
    scanSpec_->setConstantValue(
        BaseVector::createNullConstant(requestedType_, 1, pool_));
    return;
  }

  const bool useColumnNames = columnReaderOptions.columnMappingMode_ ==
      dwio::common::ColumnMappingMode::kName;
  if (useColumnNames &&
      std::all_of(childSpecs.begin(), childSpecs.end(), [&](auto* childSpec) {
        return childSpec->columnType() ==
            common::ScanSpec::ColumnType::kRegular &&
            isChildMissing(*childSpec);
      })) {
    scanSpec_->setConstantValue(
        BaseVector::createNullConstant(requestedType_, 1, pool_));
    return;
  }

  scanSpec_->setConstantValue(nullptr);
}

void StructColumnReader::ensureSyntheticRepDefSource(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    ParquetParams& params) {
  auto type = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get());
  if (!type->parent() || !children_.empty()) {
    return;
  }

  const auto* leafType = &findFirstPhysicalLeaf(*type);
  std::shared_ptr<const dwio::common::TypeWithId> leafFileType(
      fileType_, leafType);

  // Struct nullness can be derived from any leaf under the struct. Keep one
  // physical leaf reader solely as the repetition/definition level source.
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
    auto* syntheticReader =
        syntheticRepDefSource_ ? syntheticRepDefSource_->reader.get() : nullptr;
    VELOX_CHECK_NOT_NULL(
        syntheticReader,
        "Cannot source repetition/definition levels for nested struct: {}",
        fileType_->fullName());
    return syntheticReader;
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
  if (levelInfo_.defLevel == 0) {
    return;
  }
  auto repDefRange = pageReader.repDefRange();
  int32_t numRepDefs = repDefRange.second - repDefRange.first;
  dwio::common::ensureCapacity<uint64_t>(
      nullsInReadRange_, bits::nwords(numRepDefs), pool_);
  auto numStructs = pageReader.getLengthsAndNulls(
      levelMode_,
      levelInfo_,
      repDefRange.first,
      repDefRange.second,
      numRepDefs,
      nullptr,
      nullsInReadRange()->asMutable<uint64_t>(),
      0);
  // Repeated parents still need rep/def levels to determine the number of
  // structs. Preserve that count, but mark every struct null when schema
  // evolution synthesized this field as a null constant.
  if (scanSpec_->isConstant() && scanSpec_->constantValue()->isNullAt(0)) {
    bits::fillBits(
        nullsInReadRange()->asMutable<uint64_t>(), 0, numStructs, bits::kNull);
  }
  formatData_->as<ParquetData>().setNulls(nullsInReadRange(), numStructs);
}

void StructColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  for (const auto& child : children_) {
    child->filterRowGroups(rowGroupSize, context, result);
  }
}

} // namespace facebook::velox::parquet
