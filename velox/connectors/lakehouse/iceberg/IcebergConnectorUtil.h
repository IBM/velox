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

#include "velox/connectors/lakehouse/base/TableHandleBase.h"
#include "velox/dwio/common/Reader.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

#include <string>

namespace facebook::velox::connector::lakehouse::iceberg  {

bool applyPartitionFilter(
    const TypePtr& type,
    const std::string& partitionValue,
    velox::common::Filter* filter,
    bool asLocalTime);

bool filterSplit(
    const velox::common::ScanSpec* scanSpec,
    const dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionData,
    const std::unordered_map<
        std::string,
        std::shared_ptr<base::ColumnHandleBase>>& partitionKeysHandle,
    bool asLocalTime);

} // namespace facebook::velox::connector::lakehouse::iceberg
