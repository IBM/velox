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

#include "velox/connectors/hive_common/ConnectorSplitBase.h"

namespace facebook::velox::connector::hive_common {

std::string ConnectorSplitBase::getFileName() const {
  const auto i = filePath.rfind('/');
  return i == std::string::npos ? filePath : filePath.substr(i + 1);
}

folly::dynamic ConnectorSplitBase::serializeBase(
    const std::string& className) const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = className;
  obj["connectorId"] = connectorId;
  obj["splitWeight"] = splitWeight;
  obj["cacheable"] = cacheable;
  obj["filePath"] = filePath;
  obj["fileFormat"] = dwio::common::toString(fileFormat);
  obj["start"] = start;
  obj["length"] = length;

  folly::dynamic serdeParametersObj = folly::dynamic::object;
  for (const auto& [key, value] : serdeParameters) {
    serdeParametersObj[key] = value;
  }
  obj["serdeParameters"] = serdeParametersObj;

  folly::dynamic storageParametersObj = folly::dynamic::object;
  for (const auto& [key, value] : storageParameters) {
    storageParametersObj[key] = value;
  }
  obj["storageParameters"] = storageParametersObj;

  folly::dynamic infoColumnsObj = folly::dynamic::object;
  for (const auto& [key, value] : infoColumns) {
    infoColumnsObj[key] = value;
  }
  obj["infoColumns"] = infoColumnsObj;

  if (properties.has_value()) {
    folly::dynamic propertiesObj = folly::dynamic::object;
    propertiesObj["fileSize"] = properties->fileSize.has_value()
        ? folly::dynamic(properties->fileSize.value())
        : nullptr;
    propertiesObj["modificationTime"] = properties->modificationTime.has_value()
        ? folly::dynamic(properties->modificationTime.value())
        : nullptr;
    obj["properties"] = propertiesObj;
  }

  return obj;
}

std::string ConnectorSplitBase::toStringBase(const std::string& className) const {
  return fmt::format(
      "{} [{}: {} {} - {}, fileFormat: {}",
      className,
      connectorId,
      filePath,
      start,
      length,
      fileFormat);
}

} // namespace facebook::velox::connector::hive_common
