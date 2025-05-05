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

#if(${VELOX_BUILD_TESTING})
#  add_subdirectory(tests)
#elseif(${VELOX_BUILD_TEST_UTILS})
#  add_subdirectory(tests/utils)
#endif()

add_subdirectory(storage_adapters)

velox_add_library(
  velox_connector_common
  STATIC
  ConnectorConfig.cpp
  ConnectorDataSource.cpp
  ConnectorSplitBase.cpp
  ConnectorUtil.cpp
  FileHandle.cpp
  SplitReader.cpp
  TableHandle.cpp)

#velox_include_directories(velox_connectors_common PRIVATE ${Protobuf_INCLUDE_DIRS})

velox_link_libraries(
  velox_connector_common
  velox_buffer
  velox_caching
  velox_common_io
  velox_common_compression
  velox_common_config
  velox_dwio_common_encryption
  velox_dwio_common_exception
  velox_exception
  velox_expression
  velox_memory
  velox_type_tz
  Boost::regex
  Folly::folly
  glog::glog
  protobuf::libprotobuf)

if(${VELOX_BUILD_TESTING})
    add_subdirectory(tests)
endif()