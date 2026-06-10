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

#include "velox/expression/ExprSetV2.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec {

ExprSetV2::ExprSetV2(std::shared_ptr<ExprSet> /*source*/) {
  VELOX_NYI("ExprSetV2 construction lands in step 3 of the refactor.");
}

void ExprSetV2::eval(
    const SelectivityVector& /*rows*/,
    EvalCtx& /*ctx*/,
    std::vector<VectorPtr>& /*results*/) {
  VELOX_NYI("ExprSetV2::eval lands in step 4 of the refactor.");
}

} // namespace facebook::velox::exec
