# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

velox_add_library(velox_hive_config OBJECT HiveConfig.cpp)
velox_link_libraries(velox_hive_config velox_core velox_exception)

velox_add_library(velox_hive_partition_function HivePartitionFunction.cpp)
velox_link_libraries(velox_hive_partition_function velox_core velox_exec)

velox_add_library(
  velox_hive_connector
  SHARED
  HiveConfig.cpp
  HiveConnector.cpp
  HiveConnectorSplit.cpp
        HiveConnectorUtil.cpp
  HiveDataSink.cpp
  HiveDataSource.cpp
  HivePartitionUtil.cpp
  HiveSplitReader.cpp
  HiveTableHandle.cpp
  PartitionIdGenerator.cpp
        )

velox_link_libraries(
  velox_hive_connector PUBLIC velox_hive_partition_function velox_connector
                       PRIVATE velox_connector_common velox_common_io velox_dwio_common velox_dwio_catalog_fbhive )



if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
