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

#include "IcebergDeleteFile.h"
#include "velox/connectors/lakehouse/base/ConnectorSplitBase.h"

#include <string>

namespace facebook::velox::connector::lakehouse::iceberg  {

struct IcebergConnectorSplit :  public base::ConnectorSplitBase {
  std::vector<IcebergDeleteFile> deleteFiles;

//  IcebergConnectorSplit(
//      const std::string& connectorId,
//      const std::string& filePath,
//      dwio::common::FileFormat fileFormat,
//      uint64_t start = 0,
//      uint64_t length = std::numeric_limits<uint64_t>::max(),
//      const std::unordered_map<std::string, std::optional<std::string>>&
//          partitionKeys = {},
//      std::optional<int32_t> tableBucketNumber = std::nullopt,
//      const std::unordered_map<std::string, std::string>& customSplitInfo = {},
//      const std::shared_ptr<std::string>& extraFileInfo = {},
//      bool cacheable = true,
//      const std::unordered_map<std::string, std::string>& infoColumns = {},
//      std::optional<FileProperties> fileProperties = std::nullopt);

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
      std::vector<IcebergDeleteFile> _deletes = {},
      const std::unordered_map<std::string, std::string>& _infoColumns = {},
      std::optional<FileProperties> _properties = std::nullopt);
};

} // namespace facebook::velox::connector::lakehouse::iceberg
