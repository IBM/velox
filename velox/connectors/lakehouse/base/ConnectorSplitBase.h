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

#include "FileProperties.h"
#include "velox/connectors/Connector.h"
#include "velox/dwio/common/Options.h"

#include <string>

namespace facebook::velox::connector::lakehouse::base {

struct ConnectorSplitBase : public connector::ConnectorSplit {
  const std::string filePath;
  const dwio::common::FileFormat fileFormat;
  const uint64_t start;
  const uint64_t length;

  /// Mapping from partition keys to values. Values are specified as strings
  /// formatted the same way as CAST(x as VARCHAR). Null values are specified as
  /// std::nullopt. Date values must be formatted using ISO 8601 as YYYY-MM-DD.
  /// All scalar types and date type are supported.
  const std::unordered_map<std::string, std::optional<std::string>>
      partitionKeys;

  // Parameters that are provided as the serialization options.
  std::unordered_map<std::string, std::string> serdeParameters;
  // Parameters that are provided as the physical storage properties.
  std::unordered_map<std::string, std::string> storageParameters;

  /// These represent columns like $file_size, $file_modified_time that are
  /// associated with the ConnectorSplit.
  std::unordered_map<std::string, std::string> infoColumns;

  /// These represent file properties like file size that are used while opening
  /// the file handle.
  std::optional<FileProperties> properties;

  ConnectorSplitBase(
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
      const std::unordered_map<std::string, std::string>& _infoColumns = {},
      std::optional<FileProperties> _properties = std::nullopt)
      : ConnectorSplit(_connectorId, _splitWeight, _cacheable),
        filePath(_filePath),
        fileFormat(_fileFormat),
        start(_start),
        length(_length),
        partitionKeys(_partitionKeys),
        serdeParameters(_serdeParameters),
        storageParameters(_storageParameters),
        infoColumns(_infoColumns),
        properties(_properties) {}

  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<ConnectorSplitBase, T>);
    return dynamic_cast<T*>(this);
  }

  template <typename T>
  const T* as() const {
    static_assert(std::is_base_of_v<ConnectorSplitBase, T>);
    return dynamic_cast<const T*>(this);
  }

  std::string getFileName() const;

 protected:


  folly::dynamic serializeBase(const std::string& className) const;

  std::string toStringBase(
      const std::string& className) const;

};

template <typename DerivedConnectorSplitBuilder>
class ConnectorSplitBuilder {
 public:
  explicit ConnectorSplitBuilder(std::string filePath)
      : filePath_(std::move(filePath)) {
    infoColumns_["$path"] = filePath_;
  }

  ~ConnectorSplitBuilder() = default;

  DerivedConnectorSplitBuilder& start(uint64_t start) {
    start_ = start;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& length(uint64_t length) {
    length_ = length;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& splitWeight(int64_t splitWeight) {
    splitWeight_ = splitWeight;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& cacheable(bool cacheable) {
    cacheable_ = cacheable;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& fileFormat(dwio::common::FileFormat format) {
    fileFormat_ = format;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& infoColumn(
      const std::string& name,
      const std::string& value) {
    infoColumns_.emplace(std::move(name), std::move(value));
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& serdeParameters(
      const std::unordered_map<std::string, std::string>& serdeParameters) {
    serdeParameters_ = serdeParameters;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& storageParameters(
      const std::unordered_map<std::string, std::string>& storageParameters) {
    storageParameters_ = storageParameters;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& connectorId(const std::string& connectorId) {
    connectorId_ = connectorId;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

  DerivedConnectorSplitBuilder& fileProperties(FileProperties fileProperties) {
    fileProperties_ = fileProperties;
    return static_cast<DerivedConnectorSplitBuilder&>(*this);
  }

 protected:
  const std::string filePath_;
  //  dwio::common::FileFormat fileFormat_{dwio::common::FileFormat::DWRF};
  dwio::common::FileFormat fileFormat_;
  uint64_t start_{0};
  uint64_t length_{std::numeric_limits<uint64_t>::max()};
  std::unordered_map<std::string, std::string> serdeParameters_ = {};
  std::unordered_map<std::string, std::string> storageParameters_ = {};
  std::unordered_map<std::string, std::string> infoColumns_ = {};
  std::string connectorId_;
  int64_t splitWeight_{0};
  bool cacheable_{true};
  std::optional<FileProperties> fileProperties_;
};

} // namespace facebook::velox::connector::lakehouse::base
