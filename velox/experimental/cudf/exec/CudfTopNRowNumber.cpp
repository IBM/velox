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
#include "velox/experimental/cudf/exec/CudfTopNRowNumber.h"
#include "velox/experimental/cudf/exec/Utilities.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/detail/gather.hpp>
#include <cudf/filling.hpp>
#include <cudf/sorting.hpp>
#include <cudf/stream_compaction.hpp>

namespace facebook::velox::cudf_velox {

bool CudfTopNRowNumber::shouldReplace(
    const std::shared_ptr<const core::TopNRowNumberNode>& node) {
  // Only GPU-accelerate the N=1 (deduplication) case
  // For N>1, fall back to CPU
  if (node->limit() != 1) {
    return false;
  }

  // Only support row_number function (not rank or dense_rank)
  // rank and dense_rank have different semantics that don't simplify to
  // unique() for N=1
  if (node->rankFunction() !=
      core::TopNRowNumberNode::RankFunction::kRowNumber) {
    return false;
  }

  // Check that all partition key types are supported by cuDF sort/unique
  // Complex types (ARRAY, MAP, STRUCT) are not supported
  for (const auto& key : node->partitionKeys()) {
    auto kind = key->type()->kind();
    if (kind == TypeKind::ARRAY || kind == TypeKind::MAP ||
        kind == TypeKind::ROW || kind == TypeKind::UNKNOWN) {
      return false; // Fall back to CPU for unsupported types
    }
  }

  // Check that all sort key types are supported
  for (const auto& key : node->sortingKeys()) {
    auto kind = key->type()->kind();
    if (kind == TypeKind::ARRAY || kind == TypeKind::MAP ||
        kind == TypeKind::ROW || kind == TypeKind::UNKNOWN) {
      return false; // Fall back to CPU for unsupported types
    }
  }

  return true;
}

CudfTopNRowNumber::CudfTopNRowNumber(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::TopNRowNumberNode>& node)
    : exec::Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "CudfTopNRowNumber"),
      NvtxHelper(
          nvtx3::rgb{255, 140, 0}, // Dark Orange - distinguishable in profiler
          operatorId,
          fmt::format("[{}]", node->id())),
      limit_(node->limit()),
      generateRowNumber_(node->generateRowNumber()),
      inputType_(node->inputType()) {
  VELOX_CHECK_EQ(limit_, 1, "CudfTopNRowNumber only supports limit=1");

  const auto& inputType = inputType_;

  // Extract partition key column indices
  for (const auto& key : node->partitionKeys()) {
    auto channel = exec::exprToChannel(key.get(), inputType);
    VELOX_CHECK_NE(
        channel,
        kConstantChannel,
        "TopNRowNumber doesn't allow constant partition keys");
    partitionKeys_.push_back(channel);
  }

  // Extract sort key column indices and orders
  // Pattern from CudfTopN.cpp (lines 51-67)
  const auto& sortingKeys = node->sortingKeys();
  const auto& sortingOrders = node->sortingOrders();

  for (size_t i = 0; i < sortingKeys.size(); ++i) {
    auto channel = exec::exprToChannel(sortingKeys[i].get(), inputType);
    VELOX_CHECK_NE(
        channel,
        kConstantChannel,
        "TopNRowNumber doesn't allow constant sorting keys");
    sortKeys_.push_back(channel);
  }

  // Build combined key indices: partition keys first (ASC), then sort keys
  // (user order) This ensures rows with same partition key are consecutive
  // after sort
  allKeyIndices_ = partitionKeys_;
  allKeyIndices_.insert(
      allKeyIndices_.end(), sortKeys_.begin(), sortKeys_.end());

  // Build sort orders
  // Partition keys: always ASC with NULLS BEFORE (to group nulls together)
  for (size_t i = 0; i < partitionKeys_.size(); ++i) {
    columnOrders_.push_back(cudf::order::ASCENDING);
    nullOrders_.push_back(cudf::null_order::BEFORE);
  }

  // Sort keys: use user-specified orders
  //
  // Null order logic explanation:
  // cudf::null_order::BEFORE means nulls are treated as SMALLER than non-nulls
  // cudf::null_order::AFTER means nulls are treated as LARGER than non-nulls
  //
  // The XOR maps SQL semantics to cudf semantics:
  //   SQL Behavior      | Desired cudf enum
  //   ------------------+------------------
  //   ASC NULLS FIRST   | BEFORE (nulls smallest, come first in ASC)
  //   ASC NULLS LAST    | AFTER  (nulls largest, come last in ASC)
  //   DESC NULLS FIRST  | AFTER  (nulls largest, come first in DESC)
  //   DESC NULLS LAST   | BEFORE (nulls smallest, come last in DESC)
  //
  // The XOR correctly implements this:
  //   isNullsFirst ^ !isAscending -> true means BEFORE, false means AFTER
  for (size_t i = 0; i < sortingOrders.size(); ++i) {
    const auto& order = sortingOrders[i];
    columnOrders_.push_back(
        order.isAscending() ? cudf::order::ASCENDING : cudf::order::DESCENDING);
    nullOrders_.push_back(
        (order.isNullsFirst() ^ !order.isAscending())
            ? cudf::null_order::BEFORE
            : cudf::null_order::AFTER);
  }
}

void CudfTopNRowNumber::addInput(RowVectorPtr input) {
  if (input->size() == 0) {
    return;
  }

  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfInput, "Expected CudfVector input");
  inputs_.push_back(std::move(cudfInput));
}

void CudfTopNRowNumber::noMoreInput() {
  exec::Operator::noMoreInput();

  if (inputs_.empty()) {
    finished_ = true;
  }
}

RowVectorPtr CudfTopNRowNumber::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  if (inputs_.empty()) {
    finished_ = true;
    return nullptr;
  }

  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  auto stream = cudfGlobalStreamPool().get_stream();
  auto mr = cudf::get_current_device_resource_ref();

  // Concatenate all input batches
  // Use inputType_ (not outputType_) because inputs don't have row_number
  // column
  auto concatenated = getConcatenatedTable(inputs_, inputType_, stream);
  inputs_.clear();

  // Compute deduplication
  auto result = computeDeduplication(concatenated->view(), stream, mr);

  finished_ = true;
  return result;
}

CudfVectorPtr CudfTopNRowNumber::computeDeduplication(
    cudf::table_view input,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  if (input.num_rows() == 0) {
    return nullptr;
  }

  std::unique_ptr<cudf::table> result;

  if (partitionKeys_.empty()) {
    // No partition keys = single global partition
    // Just take the first row after sorting by sort keys
    auto keyView = input.select(sortKeys_);

    // Get orders for sort keys only (skip partition key orders)
    std::vector<cudf::order> sortOrders(
        columnOrders_.begin() + partitionKeys_.size(), columnOrders_.end());
    std::vector<cudf::null_order> sortNullOrders(
        nullOrders_.begin() + partitionKeys_.size(), nullOrders_.end());

    auto sortedIndices = cudf::stable_sorted_order(
        keyView, sortOrders, sortNullOrders, stream, mr);

    // Take only the first index
    auto firstIndex = cudf::split(sortedIndices->view(), {1}, stream).front();
    result = cudf::detail::gather(
        input,
        firstIndex,
        cudf::out_of_bounds_policy::DONT_CHECK,
        cudf::detail::negative_index_policy::NOT_ALLOWED,
        stream,
        mr);
  } else {
    // With partition keys: sort then unique

    // Step 1: Get sort indices for all keys (partition + sort)
    auto allKeysView = input.select(allKeyIndices_);
    auto sortedIndices = cudf::stable_sorted_order(
        allKeysView, columnOrders_, nullOrders_, stream, mr);

    // Step 2: Gather rows in sorted order
    auto sortedTable = cudf::detail::gather(
        input,
        sortedIndices->view(),
        cudf::out_of_bounds_policy::DONT_CHECK,
        cudf::detail::negative_index_policy::NOT_ALLOWED,
        stream,
        mr);

    // Step 3: Remove consecutive duplicates by partition keys, keeping first
    // Since table is sorted by partition keys, duplicates are consecutive
    // cudf::unique is optimized for pre-sorted input
    result = cudf::unique(
        sortedTable->view(),
        partitionKeys_,
        cudf::duplicate_keep_option::KEEP_FIRST,
        cudf::null_equality::EQUAL,
        stream,
        mr);
  }

  // If row number output is requested, append column of 1s
  if (generateRowNumber_) {
    auto numRows = result->num_rows();
    auto one = cudf::numeric_scalar<int64_t>(1, true, stream);
    auto rnColumn = cudf::make_column_from_scalar(one, numRows, stream, mr);

    // Append row number column to result efficiently using release()
    // to avoid deep-copying existing columns
    auto columns = result->release();
    columns.push_back(std::move(rnColumn));
    result = std::make_unique<cudf::table>(std::move(columns));
  }

  return std::make_shared<CudfVector>(
      pool(), outputType_, result->num_rows(), std::move(result), stream);
}

} // namespace facebook::velox::cudf_velox
