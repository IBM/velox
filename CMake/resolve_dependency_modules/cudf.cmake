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

include_guard(GLOBAL)

# 3.30.4 is the minimum version required by cudf
cmake_minimum_required(VERSION 3.30.4)

# Add velox_resolve_dependency_url here for rapids-cmake, rmm, and kvikio if a specific version or commit is needed.

set(VELOX_cudf_VERSION 26.04 CACHE STRING "cudf version")

set(
  VELOX_cudf_BUILD_SHA256_CHECKSUM
  268356e986e00d057fb0eaf2a5bbeaf7a21563459299f14899b0c24727b30060
)
set(
  VELOX_cudf_SOURCE_URL
  "https://github.com/mhaseeb123/cudf/archive/refs/heads/fea/velox-integration.tar.gz"
)
velox_resolve_dependency_url(cudf)

set(VELOXX_ucxx_VERSION 0.45)
set(VELOX_ucxx_BUILD_SHA256_CHECKSUM
    e603bf66a1232c4fabb666d1b4860a8ad45eda53ec92d92e830ce126eac9f912)
set(VELOX_ucxx_SOURCE_URL
    "https://github.com/rapidsai/ucxx/archive/refs/tags/v0.45.01.tar.gz")
velox_resolve_dependency_url(ucxx)

# Use block so we don't leak variables
block(SCOPE_FOR VARIABLES)
  # Setup libcudf build to not have testing components
  set(BUILD_TESTS OFF)
  set(CUDF_BUILD_TESTUTIL OFF)
  set(BUILD_SHARED_LIBS ON)

  # Add FetchContent_Declare here for rapids-cmake, rmm, and kvikio if a specific version or commit is needed.

  FetchContent_Declare(
    cudf
    URL ${VELOX_cudf_SOURCE_URL}
    URL_HASH ${VELOX_cudf_BUILD_SHA256_CHECKSUM}
    SOURCE_SUBDIR
    cpp
    UPDATE_DISCONNECTED 1
  )

  FetchContent_Declare(
    ucxx
    URL ${VELOX_ucxx_SOURCE_URL}
    URL_HASH ${VELOX_ucxx_BUILD_SHA256_CHECKSUM}
    SOURCE_SUBDIR cpp
    UPDATE_DISCONNECTED 1
  )

  FetchContent_MakeAvailable(cudf)
  FetchContent_MakeAvailable(ucxx)

  # cudf sets all warnings as errors, and therefore fails to compile with velox
  # expanded set of warnings. We selectively disable problematic warnings just for
  # cudf
  target_compile_options(
    cudf
    PRIVATE -Wno-non-virtual-dtor -Wno-missing-field-initializers -Wno-deprecated-copy -Wno-restrict
  )

  unset(BUILD_SHARED_LIBS)
  unset(BUILD_TESTING CACHE)
endblock()
