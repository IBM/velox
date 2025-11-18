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

#include "velox/experimental/cudf-exchange/tests/CudfTestData.h"
#include <functional>
#include <random>

namespace facebook::velox::cudf_exchange {

static const char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

void CudfTestData::initialize(
    const size_t numRows,
    const size_t minStringLength,
    const size_t maxStringLength) {
  VLOG(3) << "+ CudfTestData::initialize numRows:" << numRows
          << " stringLength:[" << minStringLength << ".." << maxStringLength
          << "]";
  numRows_ = numRows;
  strings_ = std::make_shared<std::vector<std::string>>();
  integers_ = std::make_shared<std::vector<uint32_t>>();
  doubles_ = std::make_shared<std::vector<float>>();

  std::random_device rd; // Non-deterministic random seed
  std::mt19937 gen(rd()); // Mersenne Twister engine
  std::uniform_int_distribution<> dist(
      minStringLength, maxStringLength); // Range [x, y] inclusive
  std::hash<std::string> hasher; // Create a hash function object for strings

  for (int i = 0; i < numRows_; i++) {
    int strLength = dist(gen);
    std::string str = genRandomStr(strLength);
    double hashValue = hasher(str);

    strings_->push_back(str);
    integers_->push_back(strLength);
    doubles_->push_back(hashValue);
  }

  for (int i = 0; i < numRows; i++) {
    VLOG(4) << "In dataTest Generated data String: " << strings_->at(i)
            << " Integer: " << integers_->at(i)
            << " Double: " << doubles_->at(i);
  }

  VLOG(3) << "- CudfTestData::initialize";
}

std::string CudfTestData::genRandomStr(const size_t len) {
  std::string rStr;
  rStr.reserve(len);
  for (int i = 0; i < len; ++i) {
    rStr += alphanum[rand() % (sizeof(alphanum) - 1)];
  }
  return rStr;
}

} // namespace facebook::velox::cudf_exchange
