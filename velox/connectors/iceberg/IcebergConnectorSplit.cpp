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

#include "velox/connectors/iceberg/IcebergConnectorSplit.h"

#include "velox/connectors/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive_common/FileProperties.h"

namespace facebook::velox::connector::iceberg {

//IcebergConnectorSplit::IcebergConnectorSplit(
//    const std::string& connectorId,
//    const std::string& filePath,
//    dwio::common::FileFormat fileFormat,
//    uint64_t start,
//    uint64_t length,
//    const std::unordered_map<std::string, std::optional<std::string>>&
//        partitionKeys,
//    std::optional<int32_t> tableBucketNumber,
//    const std::unordered_map<std::string, std::string>& customSplitInfo,
//    const std::shared_ptr<std::string>& extraFileInfo,
//    bool cacheable,
//    const std::unordered_map<std::string, std::string>& infoColumns,
//    std::optional<hive_common::FileProperties> properties)
//    : IcebergConnectorSplit(
//          connectorId,
//          filePath,
//          fileFormat,
//          start,
//          length,
//          partitionKeys,
//          tableBucketNumber,
//          customSplitInfo,
//          extraFileInfo,
//          /*serdeParameters=*/{},
//          /*storageParameters=*/{},
//          /*splitWeight=*/0,
//          cacheable,
//          infoColumns,
//          properties,
//          std::nullopt,
//          std::nullopt) {
//  // TODO: Deserialize _extraFileInfo to get deleteFiles;
//}

// For tests only
IcebergConnectorSplit::IcebergConnectorSplit(
    const std::string& _connectorId,
    const std::string& _filePath,
    dwio::common::FileFormat _fileFormat,
    uint64_t _start,
    uint64_t _length,
    const std::unordered_map<std::string, std::optional<std::string>>&
        _partitionKeys,
    const std::unordered_map<std::string, std::string>& _serdeParameters,
    const std::unordered_map<std::string, std::string>& _storageParameters,
    int64_t _splitWeight,
    bool _cacheable,
    std::vector<IcebergDeleteFile> _deletes,
    const std::unordered_map<std::string, std::string>& _infoColumns,
    std::optional<FileProperties> _properties)
    : ConnectorSplitBase(
          _connectorId,
          _filePath,
          _fileFormat,
          _start,
          _length,
          _partitionKeys,
          _serdeParameters,
          _storageParameters,
          _splitWeight,
          _cacheable,
          _infoColumns,
          _properties),
      deleteFiles(std::move(_deletes)) {}
} // namespace facebook::velox::connector::iceberg
