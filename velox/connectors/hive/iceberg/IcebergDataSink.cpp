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

#include "velox/connectors/hive/iceberg/IcebergDataSink.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/json.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "velox/common/base/Fs.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/PartitionIdGenerator.h"
#include "velox/connectors/hive/iceberg/DataFileStatsCollector.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergPartitionIdGenerator.h"
#include "velox/connectors/hive/iceberg/TransformFactory.h"
#include "velox/dwio/common/SortingWriter.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/SortBuffer.h"

#ifdef VELOX_ENABLE_PARQUET
#include "velox/connectors/hive/iceberg/IcebergParquetStatsCollector.h"
#include "velox/dwio/parquet/writer/Writer.h"
#endif

#include "velox/connectors/hive/iceberg/TransformExprBuilder.h"
#include "velox/connectors/hive/iceberg/WriterOptionsAdapter.h"
#include "velox/type/Type.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive::iceberg {

namespace {

constexpr std::string_view kNotClusteredRowsErrorMsg =
    "Incoming records violate the writer assumption that records are clustered by spec and \n by partition within each spec. Either cluster the incoming records or switch to fanout writers.\nEncountered records that belong to already closed files:\n";

#define WRITER_NON_RECLAIMABLE_SECTION_GUARD(index)       \
  memory::NonReclaimableSectionGuard nonReclaimableGuard( \
      writerInfo_[(index)]->nonReclaimableSectionHolder.get())

std::string toJson(const std::vector<folly::dynamic>& partitionValues) {
  folly::dynamic jsonObject = folly::dynamic::object();
  folly::dynamic valuesArray = folly::dynamic::array();
  for (const auto& value : partitionValues) {
    valuesArray.push_back(value);
  }
  jsonObject["partitionValues"] = valuesArray;
  return folly::toJson(jsonObject);
}

template <TypeKind Kind>
folly::dynamic extractPartitionValue(
    const VectorPtr& child,
    vector_size_t row) {
  using T = typename TypeTraits<Kind>::NativeType;
  return child->asChecked<SimpleVector<T>>()->valueAt(row);
}

template <>
folly::dynamic extractPartitionValue<TypeKind::VARCHAR>(
    const VectorPtr& child,
    vector_size_t row) {
  return child->asChecked<SimpleVector<StringView>>()->valueAt(row).str();
}

template <>
folly::dynamic extractPartitionValue<TypeKind::VARBINARY>(
    const VectorPtr& child,
    vector_size_t row) {
  return encoding::Base64::encode(
      child->asChecked<SimpleVector<StringView>>()->valueAt(row));
}

template <>
folly::dynamic extractPartitionValue<TypeKind::TIMESTAMP>(
    const VectorPtr& child,
    vector_size_t row) {
  VELOX_DCHECK(child->type()->equivalent(*TIMESTAMP()));
  return child->asChecked<SimpleVector<Timestamp>>()->valueAt(row).toMicros();
}

class IcebergFileNameGenerator : public FileNameGenerator {
 public:

  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx& connectorQueryCtx,
      bool commitRequired) const override;

  folly::dynamic serialize() const override;

  std::string toString() const override;
};

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

std::pair<std::string, std::string> IcebergFileNameGenerator::gen(
    std::optional<uint32_t> bucketId,
    const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx& connectorQueryCtx,
    bool commitRequired) const {
  auto targetFileName = insertTableHandle->locationHandle()->targetFileName();
  if (targetFileName.empty()) {
    targetFileName = fmt::format("{}", makeUuid());
  }
  auto fileFormat =
      dwio::common::FileFormatName::toName(insertTableHandle->storageFormat());
  auto fileName = fmt::format("{}.{}", targetFileName, fileFormat);
  return {fileName, fileName};
}

folly::dynamic IcebergFileNameGenerator::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "IcebergFileNameGenerator";
  return obj;
}

std::string IcebergFileNameGenerator::toString() const {
  return "IcebergFileNameGenerator";
}

} // namespace

IcebergInsertTableHandle::IcebergInsertTableHandle(
    std::vector<IcebergColumnHandlePtr> inputColumns,
    LocationHandlePtr locationHandle,
    dwio::common::FileFormat tableStorageFormat,
    IcebergPartitionSpecPtr partitionSpec,
    std::optional<common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    WriteKind writeKind,
    const std::vector<IcebergSortingColumn>& sortedBy)
    : HiveInsertTableHandle(
          std::vector<HiveColumnHandlePtr>(
              inputColumns.begin(),
              inputColumns.end()),
          std::move(locationHandle),
          tableStorageFormat,
          nullptr,
          compressionKind,
          serdeParameters,
          nullptr,
          false,
          std::make_shared<const HiveInsertFileNameGenerator>()),
      partitionSpec_(partitionSpec),
      writeKind_(writeKind),
      columnTransforms_(),
      sortedBy_(sortedBy) {
  // Data-file writes and merge writes both require the input row type to
  // have inputColumns populated (the data file sub-sink consumes them and
  // the merge sink projects them into a narrow data batch). The
  // deletion-vector path passes a synthetic (file_path, pos) row that is
  // intentionally narrower, so skip the inputColumns check for that path.
  if (writeKind_ == WriteKind::kData || writeKind_ == WriteKind::kMerge) {
    VELOX_USER_CHECK(
        !inputColumns_.empty(),
        "Input columns cannot be empty for Iceberg tables.");
  }
  VELOX_USER_CHECK_NOT_NULL(
      locationHandle_, "Location handle is required for Iceberg tables.");
  VELOX_USER_CHECK(
      isSupportedFileFormat(tableStorageFormat),
      "Unsupported file format for writing Iceberg tables: {}",
      dwio::common::FileFormatName::toName(tableStorageFormat));
}

namespace {

// Creates partition channels by mapping partition spec fields to input column
// indices. For each field in the partition spec, finds the corresponding
// partition key column in the input columns and records its index.
//
// @param inputColumns The input columns from the insert table handle.
// @param partitionSpec The Iceberg partition specification, or nullptr if
// unpartitioned.
// @return A vector of column indices representing the partition channels. Each
// index corresponds to a partition field in the spec and points to the
// matching partition key column in the input. Returns an empty vector if
// partitionSpec is nullptr.
std::vector<column_index_t> createPartitionChannels(
    const std::vector<HiveColumnHandlePtr>& inputColumns,
    const IcebergPartitionSpecPtr& partitionSpec) {
  std::vector<column_index_t> channels;
  if (!partitionSpec) {
    return channels;
  }

  // Build a map from partition key column names to their indices in the input.
  std::unordered_map<std::string, column_index_t> partitionKeyMap;
  for (auto i = 0; i < inputColumns.size(); ++i) {
    if (inputColumns[i]->isPartitionKey()) {
      partitionKeyMap[inputColumns[i]->name()] = i;
    }
  }

  // For each field in the partition spec, find its corresponding input column
  // index.
  channels.reserve(partitionSpec->fields.size());
  for (const auto& field : partitionSpec->fields) {
    if (auto it = partitionKeyMap.find(field.name);
        it != partitionKeyMap.end()) {
      channels.push_back(it->second);
    }
  }

  return channels;
}

std::vector<column_index_t> createDataChannels(
    const IcebergInsertTableHandlePtr& insertTableHandle) {
  std::vector<column_index_t> dataChannels(
      insertTableHandle->inputColumns().size());
  std::iota(dataChannels.begin(), dataChannels.end(), 0);
  return dataChannels;
}

// Creates a RowType schema for transformed partition values based on the
// partition specification. This RowType is used to wrap the transformed
// partition columns before passing them to the partition ID generator.
//
// For each partition field in the spec:
// - The column type is the result type of the partition transform (e.g.,
//   INTEGER for year transform, DATE for day transform).
// - The column name is the source column name for identity transforms, or
//   "columnName_transformName" for non-identity transforms (e.g., "birth_year"
//   for a year transform on a birth column).
//
// @param partitionSpec The Iceberg partition specification, or nullptr if
// unpartitioned.
// @return A RowType containing one column per partition field with appropriate
// names and types. Returns nullptr if partitionSpec is nullptr.
RowTypePtr createPartitionRowType(
    const IcebergPartitionSpecPtr& partitionSpec) {
  if (!partitionSpec) {
    return nullptr;
  }

  std::vector<TypePtr> partitionKeyTypes;
  std::vector<std::string> partitionKeyNames;

  // Build column names and types for each partition field.
  // Identity transforms use the source column name directly.
  // Non-identity transforms use "columnName_transformName" format.
  for (const auto& field : partitionSpec->fields) {
    partitionKeyTypes.emplace_back(field.resultType());
    std::string key = field.transformType == TransformType::kIdentity
        ? field.name
        : fmt::format(
              "{}_{}",
              field.name,
              TransformTypeName::toName(field.transformType));
    partitionKeyNames.emplace_back(std::move(key));
  }

  return ROW(std::move(partitionKeyNames), std::move(partitionKeyTypes));
}

// Builds the CommitTaskData JSON object Presto consumes for one written file.
// Iceberg "content" is derived from the sink's WriteKind: INSERT (kData) emits
// "DATA"; the V3 deletion-vector path (kDeletionVector) emits
// "POSITION_DELETES" because Iceberg classifies deletion vectors as a
// Puffin-encoded form of position deletes for catalog accounting.
folly::dynamic buildIcebergCommitData(
    std::string path,
    uint64_t fileSizeInBytes,
    folly::dynamic metrics,
    int32_t partitionSpecId,
    std::string fileFormat,
    bool isDeletionVector) {
  folly::dynamic commitData = folly::dynamic::object;
  commitData["path"] = std::move(path);
  commitData["fileSizeInBytes"] = fileSizeInBytes;
  commitData["metrics"] = std::move(metrics);
  commitData["partitionSpecJson"] = partitionSpecId;
  // Sort order evolution is not supported. Default id 0 (unsorted order).
  commitData["sortOrderId"] = 0;
  commitData["fileFormat"] = std::move(fileFormat);
  commitData["content"] = isDeletionVector ? "POSITION_DELETES" : "DATA";
  return commitData;
}

} // namespace

IcebergDataSink::IcebergDataSink(
    RowTypePtr inputType,
    IcebergInsertTableHandlePtr insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const IcebergConfigPtr& icebergConfig)
    : IcebergDataSink(
          std::move(inputType),
          insertTableHandle,
          connectorQueryCtx,
          commitStrategy,
          hiveConfig,
          createPartitionChannels(
              insertTableHandle->inputColumns(),
              insertTableHandle->partitionSpec()),
          createDataChannels(insertTableHandle),
          createPartitionRowType(insertTableHandle->partitionSpec()),
          icebergConfig) {}

IcebergDataSink::IcebergDataSink(
    RowTypePtr inputType,
    IcebergInsertTableHandlePtr insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const std::vector<column_index_t>& partitionChannels,
    const std::vector<column_index_t>& dataChannels,
    RowTypePtr partitionRowType,
    const IcebergConfigPtr& icebergConfig)
    : HiveDataSink(
          inputType,
          insertTableHandle,
          connectorQueryCtx,
          commitStrategy,
          hiveConfig,
          0,
          nullptr,
          partitionChannels,
          dataChannels,
          !partitionChannels.empty()
              ? std::make_unique<PartitionIdGenerator>(
                    partitionRowType,
                    [&partitionChannels]() {
                      std::vector<column_index_t> transformedChannels(
                          partitionChannels.size());
                      std::iota(
                          transformedChannels.begin(),
                          transformedChannels.end(),
                          0);
                      return transformedChannels;
                    }(),
                    hiveConfig->maxPartitionsPerWriters(
                        connectorQueryCtx->sessionProperties()),
                    connectorQueryCtx->memoryPool())
              : nullptr),
      partitionSpec_(insertTableHandle->partitionSpec()),
      transformEvaluator_(
          !partitionChannels.empty() ? std::make_unique<TransformEvaluator>(
                                           TransformExprBuilder::toExpressions(
                                               partitionSpec_,
                                               partitionChannels_,
                                               inputType_,
                                               icebergConfig->functionPrefix()),
                                           connectorQueryCtx_)
                                     : nullptr),
      icebergPartitionName_(
          partitionSpec_ != nullptr
              ? std::make_unique<IcebergPartitionName>(partitionSpec_)
              : nullptr),
      partitionRowType_(std::move(partitionRowType)),
      icebergInsertTableHandle_(insertTableHandle) {
  commitPartitionValue_.resize(maxOpenWriters_);
  const auto& inputColumns = insertTableHandle_->inputColumns();

  std::function<IcebergDataFileStatsSettings(
      const IcebergNestedField&, const TypePtr&, bool)>
      buildNestedField = [&](const IcebergNestedField& f,
                             const TypePtr& type,
                             bool skipBounds) -> IcebergDataFileStatsSettings {
    VELOX_CHECK_NOT_NULL(type, "Input column type cannot be null.");
    bool currentSkipBounds = skipBounds || type->isMap() || type->isArray();
    IcebergDataFileStatsSettings field(f.id, currentSkipBounds);
    if (!f.children.empty()) {
      VELOX_CHECK_EQ(f.children.size(), type->size());
      field.children.reserve(f.children.size());
      if (type->isRow()) {
        auto rowType = asRowType(type);
        for (size_t i = 0; i < f.children.size(); ++i) {
          field.children.push_back(
              std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
                  f.children[i], rowType->childAt(i), currentSkipBounds)));
        }
      } else if (type->isArray()) {
        auto arrayType = type->asArray();
        field.children.push_back(
            std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
                f.children[0], arrayType.elementType(), currentSkipBounds)));
      } else if (type->isMap()) {
        auto mapType = type->asMap();
        for (size_t i = 0; i < f.children.size(); ++i) {
          field.children.push_back(
              std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
                  f.children[i], mapType.childAt(i), currentSkipBounds)));
        }
      }
    }
    return field;
  };

  statsSettings_ = std::make_shared<
      std::vector<std::unique_ptr<dwio::common::DataFileStatsSettings>>>();
  for (const auto& columnHandle : inputColumns) {
    auto icebergColumnHandle =
        std::dynamic_pointer_cast<const IcebergColumnHandle>(columnHandle);
    VELOX_CHECK_NOT_NULL(icebergColumnHandle, "Invalid IcebergColumnHandle.");
    statsSettings_->push_back(
        std::make_unique<IcebergDataFileStatsSettings>(buildNestedField(
            icebergColumnHandle->nestedField(),
            icebergColumnHandle->dataType(),
            false)));
  }

  icebergStatsCollector_ =
      std::make_unique<DataFileStatsCollector>(statsSettings_);

  const auto& sortedBy = insertTableHandle->sortedBy();
  if (!sortedBy.empty()) {
    sortColumnIndices_.reserve(sortedBy.size());
    sortCompareFlags_.reserve(sortedBy.size());
    for (auto i = 0; i < sortedBy.size(); ++i) {
      auto columnIndex =
          inputType_->getChildIdxIfExists(sortedBy[i].sortColumn());
      if (columnIndex.has_value()) {
        sortColumnIndices_.push_back(columnIndex.value());
        sortCompareFlags_.push_back(
            {sortedBy[i].sortOrder().isNullsFirst(),
             sortedBy[i].sortOrder().isAscending(),
             false,
             CompareFlags::NullHandlingMode::kNullAsValue});
      }
    }
    sortWrite_ = !sortColumnIndices_.empty();
  }

#ifdef VELOX_ENABLE_PARQUET
  // Only initialize Parquet stats collector for Parquet format tables
  if (insertTableHandle->storageFormat() == dwio::common::FileFormat::PARQUET) {
    std::vector<IcebergColumnHandlePtr> columnHandles;
    columnHandles.reserve(insertTableHandle->inputColumns().size());
    for (auto& column : insertTableHandle->inputColumns()) {
      columnHandles.emplace_back(
          checkedPointerCast<const IcebergColumnHandle>(column));
    }
    parquetStatsCollector_ = std::make_shared<IcebergParquetStatsCollector>(
        std::move(columnHandles));
  }
#endif
}

std::vector<std::string> IcebergDataSink::commitMessage() const {
  std::vector<std::string> commitTasks;
  commitTasks.reserve(writerInfo_.size());

  for (auto i = 0; i < writerInfo_.size(); ++i) {
    const auto& writerInfo = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(writerInfo);

    // Following metadata (json format) is consumed by Presto CommitTaskData.
    // It contains the minimal subset of metadata.
    VELOX_CHECK_EQ(writerInfo->writtenFiles.size(), dataFileStats_[i].size());
    for (auto fileIdx = 0; fileIdx < writerInfo->writtenFiles.size();
         ++fileIdx) {
      const auto& fileInfo = writerInfo->writtenFiles[fileIdx];
      folly::dynamic commitData = buildIcebergCommitData(
          (fs::path(writerInfo->writerParameters.targetDirectory()) /
           fileInfo.targetFileName)
              .string(),
          fileInfo.fileSize,
          dataFileStats_[i][fileIdx]->toJson(),
          icebergInsertTableHandle_->partitionSpec()
              ? icebergInsertTableHandle_->partitionSpec()->specId
              : 0,
          toManifestFormatString(icebergInsertTableHandle_->storageFormat()),
          icebergInsertTableHandle_->writeKind() ==
              IcebergInsertTableHandle::WriteKind::kDeletionVector);
      if (!commitPartitionValue_.empty() &&
          !commitPartitionValue_[i].isNull()) {
        commitData["partitionDataJson"] = folly::toJson(
            folly::dynamic::object(
                "partitionValues", commitPartitionValue_[i]));
      }
      auto commitDataJson = folly::toJson(commitData);
      commitTasks.push_back(commitDataJson);
    }
  }
  return commitTasks;
}

void IcebergDataSink::splitInputRowsAndEnsureWriters() {
  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);

  const auto numRows = partitionIds_.size();
  for (auto row = 0; row < numRows; ++row) {
    auto id = getIcebergWriterId(row);
    uint32_t index = ensureWriter(id);

    updatePartitionRows(index, numRows, row);

    if (!partitionData_[index].empty()) {
      continue;
    }
    buildPartitionData(index);
  }

  for (auto i = 0; i < partitionSizes_.size(); ++i) {
    if (partitionSizes_[i] != 0) {
      VELOX_CHECK_NOT_NULL(partitionRows_[i]);
      partitionRows_[i]->setSize(partitionSizes_[i] * sizeof(vector_size_t));
    }
  }
}

void IcebergDataSink::computePartitionAndBucketIds(const RowVectorPtr& input) {
  VELOX_CHECK(isPartitioned());
  VELOX_CHECK_NOT_NULL(transformEvaluator_);
  VELOX_CHECK_NOT_NULL(partitionIdGenerator_);
  // Step 1: Apply transforms to input partition columns.
  auto transformedColumns = transformEvaluator_->evaluate(input);

  // Step 2: Create RowVector based on transformed columns.
  const auto& transformedRowVector = std::make_shared<RowVector>(
      connectorQueryCtx_->memoryPool(),
      partitionRowType_,
      nullptr,
      input->size(),
      std::move(transformedColumns));
  partitionIdGenerator_->run(transformedRowVector, partitionIds_);
}

std::string IcebergDataSink::getPartitionName(uint32_t partitionId) const {
  VELOX_CHECK_NOT_NULL(icebergPartitionName_);
  return icebergPartitionName_->partitionName(
      partitionId,
      partitionIdGenerator_->partitionValues(),
      partitionKeyAsLowerCase_);
}

uint32_t IcebergDataSink::ensureWriter(const WriterId& id) {
  auto writerId = HiveDataSink::ensureWriter(id);
  if (isPartitioned() && commitPartitionValue_[writerId].isNull()) {
    commitPartitionValue_[writerId] = makeCommitPartitionValue(writerId);
  }
  return writerId;
}

void IcebergDataSink::appendData(RowVectorPtr input) {
  if (fanoutEnabled_) {
    // Use base class implementation for fanout mode.
    FileDataSink::appendData(input);
    return;
  }

  // Clustered mode.
  checkRunning();
  if (!isPartitioned()) {
    const auto index = ensureWriter(WriterId::unpartitionedId());
    write(index, input);
    return;
  }

  computePartitionAndBucketIds(input);

  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);
  const auto numRows = input->size();
  uint32_t index = 0;
  for (auto row = 0; row < numRows; ++row) {
    auto id = getIcebergWriterId(row);
    index = ensureWriter(id);
    if (currentWriterId_ != index) {
      clusteredWrite(input, currentWriterId_);
      closeWriterAndCollectStats(currentWriterId_);
      completedWriterIds_.insert(currentWriterId_);
      VELOX_USER_CHECK_EQ(
          completedWriterIds_.count(index),
          0,
          "{}",
          kNotClusteredRowsErrorMsg);
      currentWriterId_ = index;
    }
    updatePartitionRows(index, numRows, row);
    buildPartitionData(index);
  }
  clusteredWrite(input, index);
}

void IcebergDataSink::buildPartitionData(int32_t index) {
  std::vector<folly::dynamic> partitionValues(partitionChannels_.size());
  auto icebergPartitionIdGenerator =
      dynamic_cast<const IcebergPartitionIdGenerator*>(
          partitionIdGenerator_.get());
  VELOX_CHECK_NOT_NULL(icebergPartitionIdGenerator);
  const RowVectorPtr transformedValues =
      icebergPartitionIdGenerator->partitionValues();
  for (auto i = 0; i < partitionChannels_.size(); ++i) {
    const auto& child = transformedValues->childAt(i);
    if (child->isNullAt(index)) {
      partitionValues[i] = nullptr;
    } else {
      partitionValues[i] = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          extractPartitionValue, child->typeKind(), child, index);
    }
  }
  partitionData_[index] = partitionValues;
}

void IcebergDataSink::clusteredWrite(RowVectorPtr input, int32_t writerIdx) {
  if (partitionSizes_[writerIdx] != 0) {
    VELOX_CHECK_NOT_NULL(partitionRows_[writerIdx]);
    partitionRows_[writerIdx]->setSize(
        partitionSizes_[writerIdx] * sizeof(vector_size_t));
  }
  const vector_size_t partitionSize = partitionSizes_[writerIdx];
  const RowVectorPtr writerInput = partitionSize == input->size()
      ? input
      : exec::wrap(partitionSize, partitionRows_[writerIdx], input);
  write(writerIdx, writerInput);
}

WriterId IcebergDataSink::getIcebergWriterId(size_t row) const {
  std::optional<int32_t> partitionId;
  if (isPartitioned()) {
    VELOX_CHECK_LT(partitionIds_[row], std::numeric_limits<uint32_t>::max());
    partitionId = static_cast<uint32_t>(partitionIds_[row]);
  }

  std::optional<int32_t> bucketId;
  if (isBucketed()) {
    bucketId = bucketIds_[row];
  }
  return WriterId{partitionId, std::nullopt};
}

std::shared_ptr<dwio::common::WriterOptions>
IcebergDataSink::createWriterOptions(size_t writerIndex) const {
  auto options = HiveDataSink::createWriterOptions(writerIndex);

  // Dispatch format-specific Iceberg overrides through the adapter so each
  // supported format (Parquet, DWRF, Nimble) gets its pre/post-processConfigs
  // hooks applied uniformly.
  const auto adapter =
      createWriterOptionsAdapter(icebergInsertTableHandle_->storageFormat());
  if (adapter != nullptr) {
    adapter->applyPreConfigs(*options);
  }

#ifdef VELOX_ENABLE_PARQUET
  // Iceberg-runtime stats collector is not a static config; wire it inline.
  if (auto parquetOptions =
          std::dynamic_pointer_cast<parquet::WriterOptions>(options)) {
    if (parquetStatsCollector_) {
      parquetOptions->parquetFieldIds =
          parquetStatsCollector_->parquetFieldIds().children;
    }
  }
#endif

  options->processConfigs(
      *hiveConfig_->config(), *connectorQueryCtx_->sessionProperties());

  if (adapter != nullptr) {
    adapter->applyPostConfigs(*options);
  }

  return options;
}

folly::dynamic IcebergDataSink::makeCommitPartitionValue(
    uint32_t writerIndex) const {
  folly::dynamic partitionValues = folly::dynamic::array();
  const auto& transformedValues = partitionIdGenerator_->partitionValues();
  for (auto i = 0; i < partitionChannels_.size(); ++i) {
    const auto& child = transformedValues->childAt(i);
    if (child->isNullAt(writerIndex)) {
      partitionValues.push_back(nullptr);
    } else {
      partitionValues.push_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          extractPartitionValue, child->typeKind(), child, writerIndex));
    }
  }
  return partitionValues;
}

void IcebergDataSink::closeWriterAndCollectStats(size_t index) {
  auto metadata = writers_[index]->close();
  const bool fileAdded = getCurrentFileBytes(index) > 0;

  // Finalize file info (capture file size, add to writtenFiles).
  finalizeWriterFile(index);

  if (!fileAdded) {
    return;
  }
#ifdef VELOX_ENABLE_PARQUET
  if (parquetStatsCollector_) {
    dataFileStats_[index].emplace_back(
        parquetStatsCollector_->aggregate(std::move(metadata)));
    return;
  }
#endif
  // ORC/DWRF (and any other format without a stats collector) path: we don't
  // have file-level metadata that exposes row count, so derive it from
  // writerInfo_->numWrittenRows. That counter accumulates across all files
  // written by this writer (rotated files included), so compute per-file
  // recordCount as the delta since the previous closeWriterAndCollectStats
  // call for this writer index.
  //
  // Without this, the manifest writes recordCount=0 for every DWRF/ORC file,
  // which makes the DELETE/UPDATE/MERGE planner believe each file is empty
  // and skip it entirely (no rewrite, no DV puffin, no visible delete).
  if (reportedRowsPerWriter_.size() <= index) {
    reportedRowsPerWriter_.resize(index + 1, 0);
  }
  const int64_t totalRows = writerInfo_[index]->numWrittenRows;
  const int64_t thisFileRows = totalRows - reportedRowsPerWriter_[index];
  reportedRowsPerWriter_[index] = totalRows;

  auto stats = std::make_shared<IcebergDataFileStatistics>();
  stats->numRecords = thisFileRows;
  // Column-level stats (min/max/null counts) are still empty here. That only
  // degrades predicate pruning (a perf optimization), not correctness. The
  // proper fix is to add an IcebergDwrfStatsCollector that mirrors the
  // Parquet path and consumes per-stripe stats from the DWRF writer footer.
  dataFileStats_[index].emplace_back(std::move(stats));
}

void IcebergDataSink::rotateWriter(size_t index) {
  VELOX_CHECK_LT(index, writers_.size());
  VELOX_CHECK_NOT_NULL(writers_[index]);

  // Ensure dataFileStats_ has an entry for this writer index.
  if (dataFileStats_.size() <= index) {
    dataFileStats_.resize(index + 1);
  }

  // Close the writer to flush the footer and obtain file metadata, then
  // aggregate Iceberg stats from the metadata. The base rotateWriter() would
  // also call writers_[index]->close() but discards the returned metadata.
  // We close the writer ourselves to capture the metadata, then reset the
  // writer to prevent double close.
  {
    const memory::NonReclaimableSectionGuard nonReclaimableGuard(
        writerInfo_[index]->nonReclaimableSectionHolder.get());
    closeWriterAndCollectStats(index);
  }

  // Release old writer. The new writer will be created lazily on the next
  // write call.
  writers_[index].reset();

  ++writerInfo_[index]->fileSequenceNumber;
}

void IcebergDataSink::closeInternal() {
  VELOX_CHECK_NE(state_, State::kRunning);
  VELOX_CHECK_NE(state_, State::kFinishing);

  TestValue::adjust(
      "facebook::velox::connector::hive::FileDataSink::closeInternal", this);

  if (state_ == State::kClosed) {
    // Ensure dataFileStats_ has entries for all writers.
    dataFileStats_.resize(writers_.size());

    for (auto i = 0; i < writers_.size(); ++i) {
      if (writers_[i] == nullptr) {
        // Writer was rotated and is null. Stats for rotated files were already
        // collected in rotateWriter(). No final file to close.
        continue;
      }
      const memory::NonReclaimableSectionGuard nonReclaimableGuard(
          writerInfo_[i]->nonReclaimableSectionHolder.get());
      closeWriterAndCollectStats(i);
    }
  } else {
    for (auto i = 0; i < writers_.size(); ++i) {
      if (writers_[i] == nullptr) {
        continue;
      }
      memory::NonReclaimableSectionGuard nonReclaimableGuard(
          writerInfo_[i]->nonReclaimableSectionHolder.get());
      writers_[i]->abort();
    }
  }
}


bool IcebergDataSink::finishWriter(int32_t index) {
  if (!sortWrite()) {
    return true;
  }

  if (writers_[index]) {
    const uint64_t startTimeMs = getCurrentTimeMs();
    if (!writers_[index]->finish()) {
      return false;
    }
    if (getCurrentTimeMs() - startTimeMs > sortWriterFinishTimeSliceLimitMs_) {
      return false;
    }
  }
  return true;
}

bool IcebergDataSink::finish() {
  // Flush is reentry state.
  setState(State::kFinishing);

  // As for now, only sorted writer needs flush buffered data. For non-sorted
  // writer, data is directly written to the underlying file writer.
  if (!sortWrite()) {
    return true;
  }

  // TODO: we might refactor to move the data sorting logic into hive data sink.
  const uint64_t startTimeMs = getCurrentTimeMs();
  for (auto i = 0; i < writers_.size(); ++i) {
    WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
    if (writers_[i] && !writers_[i]->finish()) {
      return false;
    }
    if (getCurrentTimeMs() - startTimeMs > sortWriterFinishTimeSliceLimitMs_) {
      return false;
    }
  }
  return true;
}

std::vector<std::string> IcebergDataSink::close() {
  if (state_ == State::kRunning) {
    finish();
  }
  return HiveDataSink::close();
}

std::unique_ptr<facebook::velox::dwio::common::Writer>
IcebergDataSink::maybeCreateBucketSortWriter(
    std::unique_ptr<facebook::velox::dwio::common::Writer> writer) {
  if (!sortWrite()) {
    return writer;
  }
  auto sortPool = writerInfo_.back()->sortPool.get();
  VELOX_CHECK_NOT_NULL(sortPool);
  auto sortBuffer = std::make_unique<exec::SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      sortPool,
      writerInfo_.back()->nonReclaimableSectionHolder.get(),
      connectorQueryCtx_->prefixSortConfig(),
      spillConfig_,
      writerInfo_.back()->spillStats.get());
  return std::make_unique<dwio::common::SortingWriter>(
      std::move(writer),
      std::move(sortBuffer),
      hiveConfig_->sortWriterMaxOutputRows(
          connectorQueryCtx_->sessionProperties()),
      hiveConfig_->sortWriterMaxOutputBytes(
          connectorQueryCtx_->sessionProperties()),
      sortWriterFinishTimeSliceLimitMs_);
}

IcebergSortingColumn::IcebergSortingColumn(
    const std::string& sortColumn,
    const core::SortOrder& sortOrder)
    : sortColumn_(sortColumn), sortOrder_(sortOrder) {
  VELOX_USER_CHECK(!sortColumn_.empty(), "iceberg sort column must be set.");
}

const std::string& IcebergSortingColumn::sortColumn() const {
  return sortColumn_;
}

const core::SortOrder& IcebergSortingColumn::sortOrder() const {
  return sortOrder_;
}

folly::dynamic IcebergSortingColumn::serialize() const {
  VELOX_UNREACHABLE("Unexpected code path, implement serialize() first.");
}


} // namespace facebook::velox::connector::hive::iceberg
