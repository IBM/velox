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
#pragma once

#include "ConnectorSplitBase.h"
#include "IcebergDeleteFile.h"

#include <string>

namespace facebook::velox::connector::lakehouse::iceberg {

struct IcebergConnectorSplit : public lakehouse::iceberg::ConnectorSplitBase {
  std::vector<IcebergDeleteFile> deleteFiles;

  IcebergConnectorSplit(
      const std::string& _connectorId,
      const std::string& _filePath,
      dwio::common::FileFormat _fileFormat,
      uint64_t _start = 0,
      uint64_t _length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>&
          _partitionKeys = {},
      const std::unordered_map<std::string, std::string>& _serdeParameters = {},
      const std::unordered_map<std::string, std::string>& _storageParameters =
          {},
      int64_t _splitWeight = 0,
      bool _cacheable = true,
      const std::vector<IcebergDeleteFile>& _deletes = {},
      const std::unordered_map<std::string, std::string>& _infoColumns = {},
      std::optional<FileProperties> _properties = std::nullopt);
};

class IcebergConnectorSplitBuilder {
 public:
  explicit IcebergConnectorSplitBuilder(std::string filePath)
      : filePath_{std::move(filePath)} {
    infoColumns_["$path"] = filePath_;
  }

  IcebergConnectorSplitBuilder& start(uint64_t start) {
    start_ = start;
    return *this;
  }

  IcebergConnectorSplitBuilder& length(uint64_t length) {
    length_ = length;
    return *this;
  }

  IcebergConnectorSplitBuilder& splitWeight(int64_t splitWeight) {
    splitWeight_ = splitWeight;
    return *this;
  }

  IcebergConnectorSplitBuilder& cacheable(bool cacheable) {
    cacheable_ = cacheable;
    return *this;
  }

  IcebergConnectorSplitBuilder& fileFormat(dwio::common::FileFormat format) {
    fileFormat_ = format;
    return *this;
  }

  IcebergConnectorSplitBuilder& infoColumn(
      const std::string& name,
      const std::string& value) {
    infoColumns_.emplace(std::move(name), std::move(value));
    return *this;
  }

  IcebergConnectorSplitBuilder& partitionKeys(
      const std::unordered_map<std::string, std::optional<std::string>>& partitionKeys) {
    for (const auto& partitionKey : partitionKeys) {
      this->partitionKey(partitionKey.first, partitionKey.second);
    }
    return *this;
  }

  IcebergConnectorSplitBuilder& partitionKey(
      std::string name,
      std::optional<std::string> value) {
    partitionKeys_.emplace(std::move(name), std::move(value));
    return *this;
  }

  IcebergConnectorSplitBuilder& customSplitInfo(
      const std::unordered_map<std::string, std::string>& customSplitInfo) {
    customSplitInfo_ = customSplitInfo;
    return *this;
  }

  IcebergConnectorSplitBuilder& extraFileInfo(
      const std::shared_ptr<std::string>& extraFileInfo) {
    extraFileInfo_ = extraFileInfo;
    return *this;
  }

  IcebergConnectorSplitBuilder& serdeParameters(
      const std::unordered_map<std::string, std::string>& serdeParameters) {
    serdeParameters_ = serdeParameters;
    return *this;
  }

  IcebergConnectorSplitBuilder& connectorId(const std::string& connectorId) {
    connectorId_ = connectorId;
    return *this;
  }

  IcebergConnectorSplitBuilder& fileProperties(FileProperties fileProperties) {
    fileProperties_ = fileProperties;
    return *this;
  }

  IcebergConnectorSplitBuilder& deleteFiles(std::vector<IcebergDeleteFile> deleteFiles) {
    deleteFiles_ = std::move(deleteFiles);
    return *this;
  }


  std::unique_ptr<IcebergConnectorSplit> build() const {
    return std::make_unique<IcebergConnectorSplit>(
        connectorId_,
        filePath_,
        fileFormat_, // dwio::common::FileFormat
        start_,
        length_,
        partitionKeys_,
        serdeParameters_,
        storageParameters_,
        splitWeight_,
        cacheable_,
        deleteFiles_,
        infoColumns_,
        fileProperties_);
  }

 private:
  const std::string filePath_;
  dwio::common::FileFormat fileFormat_{dwio::common::FileFormat::DWRF};
  uint64_t start_{0};
  uint64_t length_{std::numeric_limits<uint64_t>::max()};
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys_;
  std::optional<int32_t> tableBucketNumber_;
  std::unordered_map<std::string, std::string> customSplitInfo_ = {};
  std::shared_ptr<std::string> extraFileInfo_ = {};
  std::unordered_map<std::string, std::string> serdeParameters_ = {};
  std::unordered_map<std::string, std::string> storageParameters_ = {};
  std::string connectorId_;
  int64_t splitWeight_{0};
  bool cacheable_{true};
  std::vector<IcebergDeleteFile> deleteFiles_;
  std::unordered_map<std::string, std::string> infoColumns_ = {};
  std::optional<FileProperties> fileProperties_;
};

} // namespace facebook::velox::connector::lakehouse::iceberg
