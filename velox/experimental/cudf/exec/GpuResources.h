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

#include <cudf/detail/utilities/stream_pool.hpp>

#include <rmm/mr/statistics_resource_adaptor.hpp>
#include <rmm/resource_ref.hpp>

#include <cuda/memory_resource>

#include <cstdint>
#include <optional>
#include <string_view>

namespace facebook::velox::cudf_velox {

extern std::optional<cuda::mr::any_resource<cuda::mr::device_accessible>> mr_;
extern std::optional<cuda::mr::any_resource<cuda::mr::device_accessible>>
    output_mr_;

// Statistics adaptors wrapping the main (and, if distinct, output) memory
// resources so we can report live-allocated GPU bytes. statistics_resource_adaptor
// is a cuda::mr::shared_resource: it is copyable and copies share the same
// counter state, so the copies placed in mr_/output_mr_ increment the same
// counters that these read.
extern std::optional<rmm::mr::statistics_resource_adaptor> statsMr_;
extern std::optional<rmm::mr::statistics_resource_adaptor> outputStatsMr_;

/// Returns the memory resource designated for output vector allocations.
rmm::device_async_resource_ref get_output_mr();

/// Live bytes currently allocated through the cuDF/RMM memory resource(s), or
/// -1 if cuDF is not registered. Unlike cudaMemGetInfo (which reflects the
/// retained RMM pool high-water mark), this drops when queries free their
/// allocations, so it can distinguish an idle worker from a busy one.
int64_t cudfAllocatedBytes();

/**
 * @brief Creates a memory resource based on the given mode.
 *
 * @param mode rmm::mr::pool_memory_resource mode.
 * @param percent The initial percent of GPU memory to allocate for memory
 * resource.
 */
[[nodiscard]] cuda::mr::any_resource<cuda::mr::device_accessible>
createMemoryResource(std::string_view mode, int percent);

/**
 * @brief Returns the global CUDA stream pool used by cudf.
 */
[[nodiscard]] cudf::detail::cuda_stream_pool& cudfGlobalStreamPool();

} // namespace facebook::velox::cudf_velox
