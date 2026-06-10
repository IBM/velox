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
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

ExprSetV2::ExprSetV2(std::shared_ptr<ExprSet> source)
    : sourceSet_{std::move(source)} {
  VELOX_CHECK_NOT_NULL(sourceSet_, "ExprSetV2 requires a non-null ExprSet");

  roots_.reserve(sourceSet_->exprs().size());
  for (const auto& root : sourceSet_->exprs()) {
    roots_.push_back(ExprV2::from(root));
  }

  runtimeStates_ = std::make_unique<ExprRuntimeStateTree>(roots_);
}

void ExprSetV2::eval(
    const SelectivityVector& rows,
    EvalCtx& ctx,
    std::vector<VectorPtr>& results) {
  VELOX_CHECK_EQ(
      results.size(),
      roots_.size(),
      "results vector must be sized to the number of expression roots");

  for (size_t i = 0; i < roots_.size(); ++i) {
    EvalFrame frame{*roots_[i], *runtimeStates_, ctx, rows, results[i]};
    evaluator_.evaluate(frame, this);
  }
}

} // namespace facebook::velox::exec
