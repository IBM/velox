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

add_library(velox_connector_test_lib INTERFACE)

if(${VELOX_ENABLE_HIVE_CONNECTOR})
    target_link_libraries(velox_connector_test_lib INTERFACE velox_hive_connector_test_lib)
endif()

#if(${VELOX_ENABLE_ICEBERG_CONNECTOR})
#    target_link_libraries(velox_connector_test_lib PUBLIC velox_iceberg_connector_test_lib)
#endif()

add_executable(velox_connector_test ConnectorTest.cpp)
add_test(velox_connector_test velox_connector_test)
target_link_libraries(
  velox_connector_test
  velox_connector
  velox_connector_test_lib
  GTest::gtest
  GTest::gtest_main
  glog::glog
  Folly::folly)
