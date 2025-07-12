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

#include "velox/common/base/Exceptions.h"
#include "velox/connectors/lakehouse/base/ConnectorConfigBase.h"

#include <optional>
#include <string>

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::connector::lakehouse::iceberg  {

/// Hive connector configs.
class IcebergConfig : public base::ConnectorConfigBase {
public:
 enum class InsertExistingPartitionsBehavior {
   kError,
   kAppend,
   kOverwrite,
 };


 IcebergConfig(std::shared_ptr<const config::ConfigBase> config)
     : base::ConnectorConfigBase(config) {}
};

} // namespace facebook::velox::connector::lakehouse::iceberg
