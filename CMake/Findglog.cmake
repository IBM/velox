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
#
# - Try to find Glog
# Once done, this will define
#
# GLOG_FOUND - system has Glog
# GLOG_INCLUDE_DIRS - the Glog include directories
# GLOG_LIBRARIES - link these to use Glog

include(FindPackageHandleStandardArgs)
include(SelectLibraryConfigurations)

find_library(GLOG_LIBRARY_RELEASE glog PATHS ${GLOG_LIBRARYDIR})
find_library(GLOG_LIBRARY_DEBUG glogd PATHS ${GLOG_LIBRARYDIR})

find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS ${GLOG_INCLUDEDIR})

select_library_configurations(GLOG)

find_package_handle_standard_args(glog DEFAULT_MSG GLOG_LIBRARY
                                  GLOG_INCLUDE_DIR)

mark_as_advanced(GLOG_LIBRARY GLOG_INCLUDE_DIR)

set(GLOG_LIBRARIES ${GLOG_LIBRARY})
set(GLOG_INCLUDE_DIRS ${GLOG_INCLUDE_DIR})

if(NOT TARGET glog::glog)
  add_library(glog::glog UNKNOWN IMPORTED)
  set_target_properties(glog::glog PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                              "${GLOG_INCLUDE_DIRS}")
  set_target_properties(
    glog::glog PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                          IMPORTED_LOCATION "${GLOG_LIBRARIES}")
endif()
