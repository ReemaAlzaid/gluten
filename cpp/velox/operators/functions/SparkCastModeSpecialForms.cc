/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "operators/functions/SparkCastModeSpecialForms.h"

#include "velox/expression/SpecialFormRegistry.h"
#include "velox/functions/sparksql/specialforms/SparkCastExpr.h"
#include "velox/functions/sparksql/specialforms/SparkCastHooks.h"

namespace gluten {
namespace {

using namespace facebook::velox;
using facebook::velox::functions::sparksql::SparkCastExpr;
using facebook::velox::functions::sparksql::SparkCastHooks;

bool isIntegralType(const TypePtr& type) {
  return type == TINYINT() || type == SMALLINT() || type == INTEGER() ||
      type == BIGINT();
}

// Keep this in sync with Velox's SparkCastCallToSpecialForm::isAnsiSupported.
// Velox's helper is private today; this local copy is needed for expression-level
// ANSI and legacy cast modes.
bool isAnsiSupported(const TypePtr& fromType, const TypePtr& toType) {
  if (fromType->isVarchar()) {
    return toType->isBoolean() || toType->isDate() || isIntegralType(toType);
  }
  return false;
}

exec::ExprPtr makeSparkCastExpr(
    const TypePtr& type,
    exec::ExprPtr&& input,
    bool trackCpuUsage,
    bool isTryCast,
    bool allowOverflow,
    const core::QueryConfig& config) {
  return std::make_shared<SparkCastExpr>(
      type,
      std::move(input),
      trackCpuUsage,
      isTryCast,
      std::make_shared<SparkCastHooks>(config, allowOverflow));
}

class SparkAnsiCastCallToSpecialForm : public exec::CastCallToSpecialForm {
 public:
  exec::ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<exec::ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override {
    VELOX_CHECK_EQ(
        compiledChildren.size(),
        1,
        "ANSI CAST statements expect exactly 1 argument, received {}.",
        compiledChildren.size());

    const auto& fromType = compiledChildren[0]->type();
    const bool isTryCast = !isAnsiSupported(fromType, type);
    return makeSparkCastExpr(
        type,
        std::move(compiledChildren[0]),
        trackCpuUsage,
        isTryCast,
        isTryCast,
        config);
  }
};

class SparkLegacyCastCallToSpecialForm : public exec::CastCallToSpecialForm {
 public:
  exec::ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<exec::ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override {
    VELOX_CHECK_EQ(
        compiledChildren.size(),
        1,
        "LEGACY CAST statements expect exactly 1 argument, received {}.",
        compiledChildren.size());

    return makeSparkCastExpr(
        type,
        std::move(compiledChildren[0]),
        trackCpuUsage,
        true,
        true,
        config);
  }
};

} // namespace

void registerSparkCastModeSpecialForms() {
  exec::registerFunctionCallToSpecialForm(
      kSparkAnsiCast, std::make_unique<SparkAnsiCastCallToSpecialForm>());
  exec::registerFunctionCallToSpecialForm(
      kSparkLegacyCast, std::make_unique<SparkLegacyCastCallToSpecialForm>());
}

} // namespace gluten
