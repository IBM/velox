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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "GcsEmulator.h"
#include "velox/connectors/hive_common/storage_adapters/gcs/RegisterGcsFileSystem.h"
#include "velox/connectors/hive_common/storage_adapters/test_common/InsertTest.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::filesystems {
namespace {

class GcsInsertTest : public testing::Test, public test::InsertTest {
 protected:
  static void SetUpTestSuite() {
    registerGcsFileSystem();
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    connector::registerConnectorFactory(
        std::make_shared<connector::hiveV2::HiveConnectorFactory>());
    emulator_ = std::make_shared<GcsEmulator>();
    emulator_->bootstrap();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hiveV2::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                connector::hiveV2::test::kHiveConnectorId,
                emulator_->hiveConfig(),
                ioExecutor_.get());
    connector::registerConnector(hiveConnector);
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(3);
  }

  void TearDown() override {
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
    connector::unregisterConnectorFactory(
        connector::hiveV2::HiveConnectorFactory::kHiveConnectorName);
    connector::unregisterConnector(connector::hiveV2::test::kHiveConnectorId);
  }

  std::shared_ptr<GcsEmulator> emulator_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
};
} // namespace

TEST_F(GcsInsertTest, gcsInsertTest) {
  const int64_t kExpectedRows = 1'000;
  const auto gcsBucket = gcsURI(emulator_->preexistingBucketName(), "");
  runInsertTest(gcsBucket, kExpectedRows, pool());
}
} // namespace facebook::velox::filesystems

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
