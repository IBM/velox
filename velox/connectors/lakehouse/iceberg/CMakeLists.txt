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

velox_add_library(
  velox_lakehouse_iceberg_connector
        ConnectorConfigBase.cpp
        DataSourceBase.cpp
        ConnectorSplitBase.cpp
        ConnectorUtil.cpp
        FileHandle.cpp
        SplitReaderBase.cpp
        TableHandleBase.cpp
        IcebergConfig.cpp
        IcebergConnector.cpp
        IcebergConnectorSplit.cpp
        IcebergConnectorUtil.cpp
        IcebergPartitionUtil.cpp
        PartitionIdGenerator.cpp
        IcebergDataSource.cpp
        IcebergPartitionFunction.cpp
        IcebergSplitReader.cpp
        IcebergTableHandle.cpp
        PositionalDeleteFileReader.cpp
  )

velox_link_libraries(
        velox_lakehouse_iceberg_connector
        PRIVATE velox_connector
        velox_common_io
        velox_dwio_catalog_fbhive
        velox_buffer
        velox_caching
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
