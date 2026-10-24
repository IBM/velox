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

#include <gtest/gtest.h>
#include <lz4.h>

#include <vector>

#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"
#include "velox/dwio/parquet/writer/arrow/util/Compression.h"

namespace facebook::velox::parquet::arrow::util {
namespace {

constexpr int64_t kMaxBlockSize = 128 * 1024;

std::vector<uint8_t> makeInput(int64_t size) {
  // Mix repeated runs with varying bytes so the blocks compress, but not to
  // nothing.
  std::vector<uint8_t> input(size);
  for (int64_t i = 0; i < size; ++i) {
    input[i] = static_cast<uint8_t>((i / 7) % 251);
  }
  return input;
}

uint32_t loadBigEndian(const uint8_t* data) {
  return ::arrow::bit_util::FromBigEndian(
      ::arrow::util::SafeLoadAs<uint32_t>(data));
}

TEST(CompressionTest, lz4HadoopSplitsIntoBlocksHadoopCanRead) {
  auto codec = Codec::create(Compression::LZ4_HADOOP).ValueOrDie();
  for (const int64_t size :
       {int64_t{0},
        int64_t{1},
        int64_t{1'000},
        kMaxBlockSize,
        kMaxBlockSize + 1,
        int64_t{1024 * 1024 + 7}}) {
    SCOPED_TRACE(size);
    const auto input = makeInput(size);
    std::vector<uint8_t> compressed(
        codec->maxCompressedLen(size, input.data()));
    const int64_t compressedSize =
        codec
            ->compress(size, input.data(), compressed.size(), compressed.data())
            .ValueOrDie();

    // Hadoop's Lz4Codec decompresses each block on its own into a buffer of
    // 256KB by default, so every block must be small and self contained.
    std::vector<uint8_t> output(size);
    int64_t inputOffset = 0;
    int64_t outputOffset = 0;
    int64_t numBlocks = 0;
    while (inputOffset < compressedSize) {
      const uint32_t blockSize = loadBigEndian(compressed.data() + inputOffset);
      const uint32_t blockCompressedSize =
          loadBigEndian(compressed.data() + inputOffset + sizeof(uint32_t));
      inputOffset += 2 * sizeof(uint32_t);
      EXPECT_EQ(
          blockSize, std::min<int64_t>(size - outputOffset, kMaxBlockSize));
      ASSERT_EQ(
          LZ4_decompress_safe(
              reinterpret_cast<const char*>(compressed.data() + inputOffset),
              reinterpret_cast<char*>(output.data() + outputOffset),
              blockCompressedSize,
              blockSize),
          blockSize);
      inputOffset += blockCompressedSize;
      outputOffset += blockSize;
      ++numBlocks;
    }
    EXPECT_EQ(inputOffset, compressedSize);
    EXPECT_EQ(outputOffset, size);
    EXPECT_EQ(
        numBlocks,
        std::max<int64_t>(1, (size + kMaxBlockSize - 1) / kMaxBlockSize));
    EXPECT_EQ(output, input);

    std::vector<uint8_t> roundTrip(size);
    EXPECT_EQ(
        codec
            ->decompress(
                compressedSize,
                compressed.data(),
                roundTrip.size(),
                roundTrip.data())
            .ValueOrDie(),
        size);
    EXPECT_EQ(roundTrip, input);
  }
}

} // namespace
} // namespace facebook::velox::parquet::arrow::util
