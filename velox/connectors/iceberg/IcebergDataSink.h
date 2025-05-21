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

#include "velox/connectors/iceberg/IcebergTableHandle.h"
#include "velox/connectors/Connector.h"
//#include "velox/connectors/Connector.h"
//#include "velox/connectors/hiveV2/IcebergConfig.h"
//#include "velox/connectors/hiveV2/HiveTableHandle.h"
//#include "velox/connectors/hiveV2/PartitionIdGenerator.h"
//#include "velox/core/PlanNode.h"
//#include "velox/dwio/common/Options.h"
//#include "velox/dwio/common/Writer.h"
//#include "velox/dwio/common/WriterFactory.h"
//#include "velox/exec/MemoryReclaimer.h"

//#include <folly/dynamic.h>
//
//#include <shared_mutex>

namespace facebook::velox::connector::iceberg {

class IcebergDataSink : public DataSink {
 public:
  IcebergDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const ConnectorInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const std::shared_ptr<const IcebergConfig>& icebergConfig);

  void appendData(RowVectorPtr input) override {
    VELOX_NYI("IcbergDataSink not implemented yet");
  }

  bool finish() override {
    VELOX_NYI("IcbergDataSink not implemented yet");
  }

  Stats stats() const override {
    VELOX_NYI("IcbergDataSink not implemented yet");
  }

  std::vector<std::string> close() override {
    VELOX_NYI("IcbergDataSink not implemented yet");
  }

  void abort() override {
    VELOX_NYI("IcbergDataSink not implemented yet");
  }
};
}