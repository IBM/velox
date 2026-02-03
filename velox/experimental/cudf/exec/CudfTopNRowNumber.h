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

#include "velox/experimental/cudf/exec/NvtxHelper.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/exec/Operator.h"

#include <cudf/types.hpp>

namespace facebook::velox::cudf_velox {

/// GPU-accelerated TopNRowNumber operator for the N=1 (deduplication) case.
///
/// This operator implements the common CDC deduplication pattern:
///   ROW_NUMBER() OVER (PARTITION BY keys ORDER BY timestamp DESC NULLS FIRST)
///   WHERE rn = 1
///
/// For N=1, it uses an optimized algorithm:
/// 1. Sort by (partition_keys, sort_keys)
/// 2. Use cudf::unique() with KEEP_FIRST on partition keys
///
/// Only N=1 with row_number function is supported. Other cases will fail.
class CudfTopNRowNumber : public exec::Operator, public NvtxHelper {
 public:
  CudfTopNRowNumber(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::TopNRowNumberNode>& node);

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  void noMoreInput() override;

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  /// Returns true if this operator should replace the CPU operator.
  /// Only supports limit=1 (deduplication case) with row_number function.
  static bool shouldReplace(
      const std::shared_ptr<const core::TopNRowNumberNode>& node);

 private:
  /// Core deduplication algorithm for N=1
  CudfVectorPtr computeDeduplication(
      cudf::table_view input,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr);

  const int32_t limit_;
  const bool generateRowNumber_;

  // Input type (output type without row_number column if generateRowNumber)
  RowTypePtr inputType_;

  // Partition key column indices in input schema
  std::vector<cudf::size_type> partitionKeys_;

  // Sort key column indices in input schema
  std::vector<cudf::size_type> sortKeys_;

  // Combined key indices for sorting (partition keys + sort keys)
  std::vector<cudf::size_type> allKeyIndices_;

  // Sort orders for all keys
  std::vector<cudf::order> columnOrders_;
  std::vector<cudf::null_order> nullOrders_;

  // Accumulated input batches
  std::vector<CudfVectorPtr> inputs_;

  bool finished_ = false;
};

} // namespace facebook::velox::cudf_velox
