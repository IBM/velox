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

add_library(velox_lakehouse_common_test_lib
        ConnectorTestBase.cpp
        )

target_link_libraries(
        velox_lakehouse_common_test_lib
        velox_vector_test_lib
        velox_exec
        velox_exec_test_lib
        GTest::gtest
        GTest::gtest_main)

add_library(velox_lakehouse_iceberg_reader_benchmark_lib
        IcebergConnectorTestBase.cpp IcebergSplitReaderBenchmark.cpp)

target_link_libraries(
  velox_lakehouse_iceberg_reader_benchmark_lib
  velox_exec_test_lib
  velox_exec
  Folly::folly
  Folly::follybenchmark
  ${TEST_LINK_LIBS})

if(VELOX_ENABLE_BENCHMARKS)
  add_executable(velox_lakehouse_iceberg_reader_benchmark
          IcebergSplitReaderBenchmarkMain.cpp)
  target_link_libraries(
    velox_lakehouse_iceberg_reader_benchmark
    velox_lakehouse_iceberg_reader_benchmark_lib
    velox_exec_test_lib
    velox_exec
    Folly::folly
    Folly::follybenchmark
    ${TEST_LINK_LIBS})
endif()

add_executable(
  velox_lakehouse_iceberg_test IcebergReadTest.cpp
        IcebergSplitReaderBenchmarkTest.cpp IcebergConnectorTestBase.cpp PlanBuilder.cpp)

add_test(velox_lakehouse_iceberg_test velox_lakehouse_iceberg_test)

target_link_libraries(
  velox_lakehouse_iceberg_test
  velox_lakehouse_iceberg_reader_benchmark_lib
  velox_lakehouse_iceberg_connector
  velox_lakehouse_common_test_lib
  velox_dwio_common_exception
  velox_dwio_common_test_utils
  velox_vector_test_lib
  velox_vector_fuzzer
  velox_exec
  velox_exec_test_lib
  Folly::folly
  Folly::follybenchmark
  GTest::gtest
  GTest::gtest_main)

if(VELOX_ENABLE_PARQUET)
  target_link_libraries(velox_lakehouse_iceberg_test velox_dwio_parquet_writer
                        velox_dwio_parquet_reader)
endif()

add_executable(
        velox_lakehouse_common_test
        FileHandleTest.cpp)

target_link_libraries(
        velox_lakehouse_common_test
        velox_lakehouse_common_test_lib
        velox_vector_test_lib
        velox_exec
        velox_exec_test_lib
        GTest::gtest
        GTest::gtest_main)

add_test(velox_lakehouse_common_test velox_lakehouse_common_test)
