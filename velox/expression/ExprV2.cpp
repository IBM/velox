/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Copyright (c) 2026 IBM Corporation.
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

#include "velox/expression/ExprV2.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec {

// Adapter implementation lands in step 3.  Until then, calling this is a
// programmer error -- nothing should be constructing ExprV2 yet.
std::shared_ptr<ExprV2> ExprV2::from(const std::shared_ptr<Expr>& /*expr*/) {
  VELOX_NYI("ExprV2::from is implemented in step 3 of the refactor.");
}

} // namespace facebook::velox::exec
