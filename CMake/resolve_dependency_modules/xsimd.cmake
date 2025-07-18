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

if(CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
  set(
    VELOX_XSIMD_GIT_TAG
    "main"
  )
  set(
    VELOX_XSIMD_GIT_URL
    "git@github.ibm.com:lakehouse/xsimd-ppc64le.git"
  )

  message(STATUS "Building xsimd from git")
  FetchContent_Declare(
    xsimd
    GIT_REPOSITORY ${VELOX_XSIMD_GIT_URL}
    GIT_TAG  ${VELOX_XSIMD_GIT_TAG}
  )
else()
  set(VELOX_XSIMD_VERSION 10.0.0)
  set(
    VELOX_XSIMD_BUILD_SHA256_CHECKSUM
    73f818368b3a4dad92fab1b2933d93694241bd2365a6181747b2df1768f6afdd
  )
  set(
    VELOX_XSIMD_SOURCE_URL
    "https://github.com/xtensor-stack/xsimd/archive/refs/tags/${VELOX_XSIMD_VERSION}.tar.gz"
  )

  velox_resolve_dependency_url(XSIMD)

  message(STATUS "Building xsimd from source")
  FetchContent_Declare(
    xsimd
    URL ${VELOX_XSIMD_SOURCE_URL}
    URL_HASH ${VELOX_XSIMD_BUILD_SHA256_CHECKSUM}
  )
endif()
FetchContent_MakeAvailable(xsimd)
