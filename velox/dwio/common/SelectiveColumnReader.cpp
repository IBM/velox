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

#include "velox/common/time/CpuWallTimer.h"
#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

namespace facebook::velox::dwio::common {

using dwio::common::TypeWithId;

velox::common::AlwaysTrue& alwaysTrue() {
  static velox::common::AlwaysTrue alwaysTrue;
  return alwaysTrue;
}

dwio::common::NoHook& noHook() {
  static dwio::common::NoHook hook;
  return hook;
}

void ScanState::updateRawState() {
  rawState.dictionary.values =
      dictionary.values ? dictionary.values->as<void>() : nullptr;
  rawState.dictionary.numValues = dictionary.numValues;
  rawState.dictionary2.values =
      dictionary2.values ? dictionary2.values->as<void*>() : nullptr;
  rawState.dictionary2.numValues = dictionary2.numValues;
  rawState.inDictionary = inDictionary ? inDictionary->as<uint64_t>() : nullptr;
  rawState.filterCache = filterCache.data();
}

SelectiveColumnReader::SelectiveColumnReader(
    const TypePtr& requestedType,
    std::shared_ptr<const dwio::common::TypeWithId> fileType,
    dwio::common::FormatParams& params,
    velox::common::ScanSpec& scanSpec)
    : pool_(&params.pool()),
      requestedType_(requestedType),
      fileType_(fileType),
      formatData_(params.toFormatData(fileType, scanSpec)),
      scanSpec_(&scanSpec),
      outputRows_(pool_),
      valueRows_(pool_),
      outerNonNullRows_(pool_),
      innerNonNullRows_(pool_) {
  scanState_.rowsCopy = raw_vector<vector_size_t>(pool_);
  scanState_.filterCache = raw_vector<uint8_t>(pool_);
  // Initialize per-column decoding statistics if collection is enabled.
  if (params.runtimeStatistics().decodingStatsSet) {
    decodingStats_ = params.runtimeStatistics().decodingStatsSet->getOrCreate(
        fileType_->id());
  }
}

void SelectiveColumnReader::readWithTiming(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  if (decodingStats_ && fileType_->type()->isPrimitiveType()) {
    DeltaCpuWallTimer timer([this](const CpuWallTiming& timing) {
      decodingStats_->decodeCPUTimeNanos.increment(timing.cpuNanos);
    });
    read(offset, rows, incomingNulls);
  } else {
    read(offset, rows, incomingNulls);
  }
}

void SelectiveColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    FormatData::FilterRowGroupsResult& result) const {
  formatData_->filterRowGroups(*scanSpec_, rowGroupSize, context, result);
}

const std::vector<SelectiveColumnReader*>& SelectiveColumnReader::children()
    const {
  static std::vector<SelectiveColumnReader*> empty;
  return empty;
}

void SelectiveColumnReader::seekTo(int64_t offset, bool readsNullsOnly) {
  if (offset == readOffset_) {
    return;
  }
  if (readOffset_ < offset) {
    if (numParentNulls_ > 0) {
      VELOX_CHECK_LE(
          parentNullsRecordedTo_,
          offset,
          "Must not seek to before parentNullsRecordedTo_");
    }
    const auto distance = offset - readOffset_ - numParentNulls_;
    numParentNulls_ = 0;
    parentNullsRecordedTo_ = 0;
    if (readsNullsOnly) {
      formatData_->skipNulls(distance, true);
    } else {
      skip(distance);
    }
    readOffset_ = offset;
  } else {
    VELOX_FAIL(
        "Seeking backward on a ColumnReader from {} to {}",
        readOffset_,
        offset);
  }
}

void SelectiveColumnReader::setReturnNullsMode(const RowSet& rows) {
  if (useBulkPath() && !scanSpec_->hasFilter()) {
    anyNulls_ = nullsInReadRange_ != nullptr;
    const bool isDense = rows.back() == rows.size() - 1;
    returnReaderNulls_ = anyNulls_ && isDense;
  } else {
    returnReaderNulls_ = false;
  }
}

void SelectiveColumnReader::prepareNulls(
    const RowSet& rows,
    bool hasNulls,
    int32_t extraRows) {
  if (!hasNulls) {
    anyNulls_ = false;
    return;
  }

  setReturnNullsMode(rows);
  if (returnReaderNulls_) {
    // No need for null flags if fast path.
    return;
  }

  const auto numRows = rows.size() + extraRows;
  if (resultNulls_ && resultNulls_->unique() &&
      resultNulls_->capacity() >= bits::nbytes(numRows) + simd::kPadding) {
    resultNulls_->setSize(bits::nbytes(numRows));
  } else {
    resultNulls_ =
        AlignedBuffer::allocate<bool>(numRows + (simd::kPadding * 8), pool_);
    rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
  }
  anyNulls_ = false;
  // Clear whole capacity because future uses could hit uncleared data between
  // capacity() and 'numBytes'.
  simd::memset(rawResultNulls_, bits::kNotNullByte, resultNulls_->capacity());
}

const uint64_t* SelectiveColumnReader::shouldMoveNulls(const RowSet& rows) {
  if (rows.size() == numValues_ || !anyNulls_) {
    // Nulls will only be moved if there is a selection on values. A cast
    // alone does not move nulls.
    return nullptr;
  }
  const uint64_t* moveFrom = rawResultNulls_;
  if (returnReaderNulls_) {
    if (!(resultNulls_ && resultNulls_->unique() &&
          resultNulls_->capacity() >= rows.size() + simd::kPadding)) {
      resultNulls_ = AlignedBuffer::allocate<bool>(
          rows.size() + (simd::kPadding * 8), pool_);
      rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
    }
    moveFrom = nullsInReadRange_->as<uint64_t>();
    bits::copyBits(moveFrom, 0, rawResultNulls_, 0, rows.size());
    returnReaderNulls_ = false;
  }
  VELOX_CHECK(resultNulls_ && resultNulls_->as<uint64_t>() == rawResultNulls_);
  VELOX_CHECK_GT(resultNulls_->capacity() * 8, rows.size());
  return moveFrom;
}

void SelectiveColumnReader::setComplexNulls(
    const RowSet& rows,
    VectorPtr& result) const {
  if (!nullsInReadRange_) {
    if (result->isNullsWritable()) {
      result->clearNulls(0, rows.size());
    } else {
      result->resetNulls();
    }
    return;
  }

  const bool dense = 1 + rows.back() == rows.size();
  auto& nulls = result->nulls();
  if (dense &&
      !(nulls && nulls->isMutable() &&
        nulls->capacity() >= bits::nbytes(rows.size()))) {
    result->setNulls(nullsInReadRange_);
    return;
  }

  auto* readerNulls = nullsInReadRange_->as<uint64_t>();
  auto* resultNulls = result->mutableNulls(rows.size())->asMutable<uint64_t>();
  if (dense) {
    bits::copyBits(readerNulls, 0, resultNulls, 0, rows.size());
    return;
  }
  for (vector_size_t i = 0; i < rows.size(); ++i) {
    bits::setBit(resultNulls, i, bits::isBitSet(readerNulls, rows[i]));
  }
}

void SelectiveColumnReader::getIntValues(
    const RowSet& rows,
    const TypePtr& requestedType,
    VectorPtr* result) {
  switch (requestedType->kind()) {
    case TypeKind::SMALLINT:
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int16_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<int32_t, int16_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int16_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::TINYINT:
      switch (valueSize_) {
        case 4:
          getFlatValues<int32_t, int8_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int8_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::INTEGER:
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int32_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<int32_t, int32_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int32_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::HUGEINT:
      switch (valueSize_) {
        case 16:
          getFlatValues<int128_t, int128_t>(rows, result, requestedType);
          break;
        case 8:
          getFlatValues<int64_t, int128_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<int32_t, int128_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::BIGINT:
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int64_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<int32_t, int64_t>(rows, result, requestedType);
          break;
        case 2:
          getFlatValues<int16_t, int64_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::DOUBLE:
      // Only Parquet INT32 (valueSize_==4) widens to DOUBLE. INT64->DOUBLE
      // is rejected in convertType due to precision loss.
      switch (valueSize_) {
        case 4:
          getFlatValues<int32_t, double>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::TIMESTAMP:
      // DateType inherits from IntegerType (kind() == INTEGER), and DWRF
      // erases DATE to plain INT during write -- so the genuine-DATE case
      // and the post-DWRF-roundtrip case both present as kind() == INTEGER
      // here.  Conversion (days * 86400) is the same in both.
      VELOX_CHECK_EQ(
          fileType_->type()->kind(),
          TypeKind::INTEGER,
          "TIMESTAMP output requires an INTEGER-kind file type (DATE or INTEGER), got: {}",
          fileType_->type()->toString());
      if (valueSize_ == sizeof(Timestamp)) {
        // A prior convertDateToTimestampValues call already bulk-transmuted
        // the int32 days buffer to Timestamps; further extractions are plain
        // Timestamp compactions.
        getFlatValues<Timestamp, Timestamp>(rows, result, TIMESTAMP());
      } else {
        convertDateToTimestampValues(rows, result);
      }
      break;
    default:
      VELOX_FAIL(
          "Not a valid type for integer reader: {}", requestedType->toString());
  }
}

void SelectiveColumnReader::getUnsignedIntValues(
    const RowSet& rows,
    const TypePtr& requestedType,
    VectorPtr* result) {
  switch (requestedType->kind()) {
    case TypeKind::TINYINT:
      switch (valueSize_) {
        case 1:
          getFlatValues<uint8_t, uint8_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<uint32_t, uint8_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::SMALLINT:
      switch (valueSize_) {
        case 2:
          getFlatValues<uint16_t, uint16_t>(rows, result, requestedType);
          break;
        case 4:
          getFlatValues<uint32_t, uint16_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::INTEGER:
      switch (valueSize_) {
        case 4:
          getFlatValues<uint32_t, uint32_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::BIGINT:
      switch (valueSize_) {
        case 4:
          getFlatValues<uint32_t, uint64_t>(rows, result, requestedType);
          break;
        case 8:
          getFlatValues<uint64_t, uint64_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    case TypeKind::HUGEINT:
      switch (valueSize_) {
        case 8:
          getFlatValues<uint64_t, uint128_t>(rows, result, requestedType);
          break;
        case 16:
          getFlatValues<uint128_t, uint128_t>(rows, result, requestedType);
          break;
        default:
          VELOX_FAIL("Unsupported value size: {}", valueSize_);
      }
      break;
    default:
      VELOX_FAIL(
          "Not a valid type for unsigned integer reader: {}",
          requestedType->toString());
  }
}

template <>
void SelectiveColumnReader::getFlatValues<int8_t, bool>(
    const RowSet& rows,
    VectorPtr* result,
    const TypePtr& type,
    bool isFinal) {
  constexpr int32_t kWidth = xsimd::batch<int8_t>::size;
  VELOX_CHECK_EQ(valueSize_, sizeof(int8_t));
  compactScalarValues<int8_t, int8_t>(rows, isFinal);
  auto boolValues = AlignedBuffer::allocate<bool>(numValues_, pool_, false);
  auto rawBytes = values_->as<int8_t>();
  auto zero = xsimd::broadcast<int8_t>(0);
  if constexpr (kWidth == 32) {
    auto rawBits = boolValues->asMutable<uint32_t>();
    for (auto i = 0; i < numValues_; i += kWidth) {
      rawBits[i / kWidth] =
          ~simd::toBitMask(zero == xsimd::load_unaligned(rawBytes + i));
    }
  } else {
    VELOX_DCHECK_EQ(kWidth, 16);
    auto rawBits = boolValues->asMutable<uint16_t>();
    for (auto i = 0; i < numValues_; i += kWidth) {
      rawBits[i / kWidth] =
          ~simd::toBitMask(zero == xsimd::load_unaligned(rawBytes + i));
    }
  }
  *result = std::make_shared<FlatVector<bool>>(
      pool_,
      type,
      resultNulls(),
      numValues_,
      std::move(boolValues),
      std::move(stringBuffers_));
}

template <>
void SelectiveColumnReader::compactScalarValues<bool, bool>(
    const RowSet& rows,
    bool isFinal) {
  if (!values_ || rows.size() == numValues_) {
    if (values_) {
      values_->setSize(bits::nbytes(numValues_));
    }
    return;
  }
  auto rawBits = reinterpret_cast<uint64_t*>(rawValues_);
  vector_size_t rowIndex = 0;
  auto nextRow = rows[rowIndex];
  auto* moveNullsFrom = shouldMoveNulls(rows);
  for (size_t i = 0; i < numValues_; i++) {
    if (outputRows_[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK_EQ(outputRows_[i], nextRow);

    bits::setBit(rawBits, rowIndex, bits::isBitSet(rawBits, i));
    if (moveNullsFrom && rowIndex != i) {
      bits::setBit(rawResultNulls_, rowIndex, bits::isBitSet(moveNullsFrom, i));
    }
    if (!isFinal) {
      outputRows_[rowIndex] = nextRow;
    }
    rowIndex++;
    if (rowIndex >= rows.size()) {
      break;
    }
    nextRow = rows[rowIndex];
  }
  numValues_ = rows.size();
  outputRows_.resize(numValues_);
  values_->setSize(bits::nbytes(numValues_));
}

void SelectiveColumnReader::convertDateToTimestampValues(
    const RowSet& rows,
    VectorPtr* result,
    bool isFinal) {
  VELOX_CHECK_EQ(valueSize_, sizeof(int32_t));
  VELOX_CHECK(mayGetValues_);

  if (allNull_) {
    *result = std::make_shared<ConstantVector<Timestamp>>(
        pool_, rows.size(), true, TIMESTAMP(), Timestamp());
    if (isFinal) {
      mayGetValues_ = false;
    }
    return;
  }
  VELOX_CHECK_NOT_NULL(values_);

  static constexpr int64_t kSecondsPerDay{86'400};

  // Bulk path: this single call asks for at least half of the read values.
  // Transmute all numValues_ days to Timestamps in place (amortizing the
  // conversion), transition to State B, then emit the requested slice via
  // standard Timestamp extraction. Iterates backwards so the wider
  // Timestamp writes don't clobber int32 source bytes we haven't read yet.
  //
  // Reentrancy note: after this path, subsequent calls go through
  // getFlatValues<Timestamp, Timestamp> (dispatched via valueSize_ ==
  // sizeof(Timestamp) in getIntValues). getFlatValues -> compactScalarValues
  // shrinks values_ to rows.size() and updates numValues_ / valueRows_ in
  // place, discarding un-extracted rows. Any follow-up call must therefore
  // ask for rows that advance forward through what remains; disjoint or
  // earlier subsets read stale/undefined bytes. The small-subset path below
  // does not have this constraint.
  if (rows.size() >= numValues_ / 2) {
    ensureValuesCapacity<Timestamp>(numValues_, true);
    const auto* rawDays = reinterpret_cast<const int32_t*>(rawValues_);
    auto* timestamps = reinterpret_cast<Timestamp*>(rawValues_);
    for (auto i = numValues_; i-- > 0;) {
      timestamps[i] =
          Timestamp(kSecondsPerDay * static_cast<int64_t>(rawDays[i]), 0);
    }
    valueSize_ = sizeof(Timestamp);
    values_->setSize(numValues_ * sizeof(Timestamp));
    getFlatValues<Timestamp, Timestamp>(rows, result, TIMESTAMP(), isFinal);
    return;
  }

  // Small-subset path: leave the int32 days buffer intact and emit a
  // freshly-allocated Timestamp vector for just rows.size(). values_,
  // valueSize_, numValues_, and valueRows_ are all untouched, so this path
  // supports arbitrary follow-up subsets of the same read -- including
  // disjoint or earlier ones -- until a call happens to hit the bulk path
  // above and switches valueSize_ to sizeof(Timestamp).
  const auto sourceRows = valueRows_.empty()
      ? (outputRows_.empty() ? RowSet(inputRows_) : RowSet(outputRows_))
      : RowSet(valueRows_);
  const auto* rawDays = reinterpret_cast<const int32_t*>(rawValues_);

  auto timestampValues = AlignedBuffer::allocate<Timestamp>(
      rows.size() + simd::kPadding / sizeof(Timestamp), pool_);
  auto* timestamps = timestampValues->asMutable<Timestamp>();

  const auto* moveNullsFrom = shouldMoveNulls(rows);
  BufferPtr localNullsBuffer;
  uint64_t* localNulls = nullptr;
  if (moveNullsFrom) {
    localNullsBuffer = AlignedBuffer::allocate<bool>(rows.size(), pool_);
    localNulls = localNullsBuffer->asMutable<uint64_t>();
  }

  size_t sourceIndex{0};
  if (moveNullsFrom) {
    for (vector_size_t rowIndex = 0; rowIndex < rows.size();) {
      const auto begin = rowIndex;
      const auto end = std::min<vector_size_t>(begin + 64, rows.size());
      uint64_t nulls = 0;
      for (; rowIndex < end; ++rowIndex) {
        while (sourceRows[sourceIndex] < rows[rowIndex]) {
          ++sourceIndex;
        }
        VELOX_DCHECK_EQ(sourceRows[sourceIndex], rows[rowIndex]);
        timestamps[rowIndex] = Timestamp(
            kSecondsPerDay * static_cast<int64_t>(rawDays[sourceIndex]), 0);
        nulls |=
            static_cast<uint64_t>(bits::isBitSet(moveNullsFrom, sourceIndex))
            << (rowIndex - begin);
        ++sourceIndex;
      }
      localNulls[begin / 64] = nulls;
    }
  } else {
    for (vector_size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
      while (sourceRows[sourceIndex] < rows[rowIndex]) {
        ++sourceIndex;
      }
      VELOX_DCHECK_EQ(sourceRows[sourceIndex], rows[rowIndex]);
      timestamps[rowIndex] = Timestamp(
          kSecondsPerDay * static_cast<int64_t>(rawDays[sourceIndex++]), 0);
    }
  }

  *result = std::make_shared<FlatVector<Timestamp>>(
      pool_,
      TIMESTAMP(),
      moveNullsFrom ? localNullsBuffer : resultNulls(),
      rows.size(),
      std::move(timestampValues),
      std::vector<BufferPtr>{});

  if (isFinal) {
    mayGetValues_ = false;
  }
}

char* SelectiveColumnReader::copyStringValue(std::string_view value) {
  uint64_t size = value.size();
  if (stringBuffers_.empty() || rawStringUsed_ + size > rawStringSize_) {
    auto bytes = std::max(size, kStringBufferSize);
    BufferPtr buffer = AlignedBuffer::allocate<char>(bytes, pool_);
    // Use the preferred size instead of the requested one to improve memory
    // efficiency.
    buffer->setSize(buffer->capacity());
    stringBuffers_.push_back(buffer);
    rawStringBuffer_ = buffer->asMutable<char>();
    rawStringUsed_ = 0;
    // Adjust the size downward so that the last store can take place
    // at full width.
    rawStringSize_ = buffer->capacity() - simd::kPadding;
  }
  memcpy(rawStringBuffer_ + rawStringUsed_, value.data(), size);
  auto start = rawStringUsed_;
  rawStringUsed_ += size;
  return rawStringBuffer_ + start;
}

void SelectiveColumnReader::addStringValue(std::string_view value) {
  auto copy = copyStringValue(value);
  reinterpret_cast<StringView*>(rawValues_)[numValues_++] =
      StringView(copy, value.size());
}

void SelectiveColumnReader::setNulls(BufferPtr resultNulls) {
  resultNulls_ = resultNulls;
  rawResultNulls_ = resultNulls ? resultNulls->asMutable<uint64_t>() : nullptr;
  anyNulls_ = rawResultNulls_ &&
      !bits::isAllSet(rawResultNulls_, 0, numValues_, bits::kNotNull);
  allNull_ =
      anyNulls_ && bits::isAllSet(rawResultNulls_, 0, numValues_, bits::kNull);
  returnReaderNulls_ = false;
}

void SelectiveColumnReader::resetFilterCaches() {
  if (scanState_.filterCache.empty() && scanSpec_->hasFilter()) {
    scanState_.filterCache.resize(
        std::max<int32_t>(
            1,
            scanState_.dictionary.numValues +
                scanState_.dictionary2.numValues));
    scanState_.updateRawState();
  }
  if (!scanState_.filterCache.empty()) {
    simd::memset(
        scanState_.filterCache.data(),
        FilterResult::kUnknown,
        scanState_.filterCache.size());
  }
}

void SelectiveColumnReader::addParentNulls(
    int64_t firstRowInNulls,
    const uint64_t* nulls,
    const RowSet& rows) {
  const int32_t firstNullIndex =
      readOffset_ < firstRowInNulls ? 0 : readOffset_ - firstRowInNulls;
  numParentNulls_ +=
      nulls ? bits::countNulls(nulls, firstNullIndex, rows.back() + 1) : 0;
  parentNullsRecordedTo_ = firstRowInNulls + rows.back() + 1;
}

void SelectiveColumnReader::addSkippedParentNulls(
    int64_t from,
    int64_t to,
    int32_t numNulls) {
  const auto rowsPerRowGroup = formatData_->rowsPerRowGroup();
  if (rowsPerRowGroup.has_value() &&
      from / rowsPerRowGroup.value() >
          parentNullsRecordedTo_ / rowsPerRowGroup.value()) {
    // the new nulls are in a different row group than the last.
    parentNullsRecordedTo_ = from;
    numParentNulls_ = 0;
  }
  if (parentNullsRecordedTo_ > 0) {
    VELOX_CHECK_EQ(parentNullsRecordedTo_, from);
  }
  numParentNulls_ += numNulls;
  parentNullsRecordedTo_ = to;
}

} // namespace facebook::velox::dwio::common
