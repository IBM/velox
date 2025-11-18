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

#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>
#include "velox/type/Type.h"

namespace facebook::velox::cudf_exchange {

class CudfTestData {
 public:
  inline static const std::vector<std::string> kTestColumnNames = {
      "c0",
      "c1",
      "c2"};
  inline const static std::vector<TypePtr> kTestColumnTypes = {
      INTEGER(),
      DOUBLE(),
      VARCHAR()};
  inline const static facebook::velox::RowTypePtr kTestRowType =
      ROW(kTestColumnNames, kTestColumnTypes);

  // Make a constant to avoid too many variables
  static const int STRING_LENGTH = 4;

  CudfTestData() = default;
  void initialize(
      const size_t numRows,
      const size_t minStringLength = CudfTestData::STRING_LENGTH) {
    initialize(numRows, minStringLength, minStringLength);
  }

  void initialize(
      const size_t numRows,
      const size_t minStringLength,
      const size_t maxStringLength);

  std::shared_ptr<std::vector<std::string>> getStrings() {
    return strings_;
  }

  std::shared_ptr<std::vector<uint32_t>> getIntegers() {
    return integers_;
  }

  std::shared_ptr<std::vector<float>> getDoubles() {
    return doubles_;
  }

  size_t getNumRows() {
    return numRows_;
  }

 protected:
  std::string genRandomStr(const size_t len);

  std::shared_ptr<std::vector<std::string>> strings_;
  std::shared_ptr<std::vector<uint32_t>> integers_;
  std::shared_ptr<std::vector<float>> doubles_;
  size_t numRows_;
};

} // namespace facebook::velox::cudf_exchange
