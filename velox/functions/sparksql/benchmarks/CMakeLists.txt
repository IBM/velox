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
add_executable(velox_sparksql_benchmarks_in In.cpp)

target_link_libraries(
  velox_sparksql_benchmarks_in
  velox_functions_prestosql
  velox_functions_spark
  velox_expression
  velox_exec_test_lib
  velox_vector_test_lib
  velox_vector_fuzzer
  Folly::folly
  Folly::follybenchmark)

add_executable(velox_sparksql_benchmarks_compare CompareBenchmark.cpp)
target_link_libraries(
  velox_sparksql_benchmarks_compare velox_functions_spark
  velox_benchmark_builder velox_vector_test_lib)

add_executable(velox_sparksql_benchmarks_simd_compare SIMDCompareBenchmark.cpp)
target_link_libraries(
  velox_sparksql_benchmarks_simd_compare velox_functions_spark
  velox_benchmark_builder velox_vector_test_lib)

add_executable(velox_sparksql_benchmarks_hash HashBenchmark.cpp)
target_link_libraries(
  velox_sparksql_benchmarks_hash velox_functions_spark velox_benchmark_builder
  velox_vector_test_lib)

add_executable(velox_sparksql_benchmarks_from_json FromJsonBenchmark.cpp)
target_link_libraries(
  velox_sparksql_benchmarks_from_json
  velox_functions_spark
  velox_exec_test_lib
  velox_parse_utils
  Folly::folly
  Folly::follybenchmark)
