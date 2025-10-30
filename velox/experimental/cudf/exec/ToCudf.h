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

#include "velox/experimental/cudf-exchange/ExchangeClientFacade.h"

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"

#include <rmm/mr/device_memory_resource.hpp>

#include <gflags/gflags.h>

DECLARE_bool(velox_cudf_enabled);
DECLARE_string(velox_cudf_memory_resource);
DECLARE_bool(velox_cudf_debug);
DECLARE_bool(velox_cudf_exchange);

namespace facebook::velox::cudf_velox {

static const std::string kCudfAdapterName = "cuDF";

// QueryConfig key. Enable or disable cudf in task level.
static const std::string kCudfEnabled = "cudf.enabled";

struct TaskPlanNodeKey {
  std::string taskId;
  core::PlanNodeId planNodeId;

  TaskPlanNodeKey(const std::string& tid, const core::PlanNodeId& pid)
      : taskId(tid), planNodeId(pid) {}

  // need equality operator for unordered map.
  bool operator==(const TaskPlanNodeKey& other) const {
    return taskId == other.taskId && planNodeId == other.planNodeId;
  }

  // Need a hash functor for the unordered map.
  struct Hash {
    std::size_t operator()(const TaskPlanNodeKey& key) const {
      std::hash<std::string> hasher;
      std::size_t h1 = hasher(key.taskId);
      std::size_t h2 = hasher(key.planNodeId);
      return h1 ^ (h2 << 1); // simple combination of the two hash functions.
    }
  };
};

static std::unordered_map<
    TaskPlanNodeKey,
    std::shared_ptr<cudf_exchange::ExchangeClientFacade>,
    TaskPlanNodeKey::Hash>
    exchangeClientFacadeByTaskAndPlanNode_;

class CompileState {
 public:
  CompileState(const exec::DriverFactory& driverFactory, exec::Driver& driver)
      : driverFactory_(driverFactory), driver_(driver) {}

  exec::Driver& driver() {
    return driver_;
  }

  // Replaces sequences of Operators in the Driver given at construction with
  // cuDF equivalents. Returns true if the Driver was changed.
  bool compile(bool allow_cpu_fallback);

  const exec::DriverFactory& driverFactory_;
  exec::Driver& driver_;
};

extern std::shared_ptr<rmm::mr::device_memory_resource> mr_;

/// Registers adapter to add cuDF operators to Drivers.
void registerCudf();
void unregisterCudf();

/// Returns true if cuDF is registered.
bool cudfIsRegistered();

} // namespace facebook::velox::cudf_velox
