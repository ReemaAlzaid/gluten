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

#include "cudf/GlutenCudfFunctions.h"
#include "operators/functions/RowConstructorWithNull.h"
#include "velox/experimental/cudf/exec/GpuResources.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace gluten {
namespace {

using namespace facebook::velox;

class GlutenCudfFunctionsTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    cudf_velox::registerCudf();
    registerGlutenCudfFunctions();
  }

  static void TearDownTestCase() {
    cudf_velox::unregisterFunctions();
    cudf_velox::unregisterCudf();
  }

  RowVectorPtr evaluateOnGpu(const core::TypedExprPtr& expr, const RowVectorPtr& input) {
    auto stream = cudf_velox::cudfGlobalStreamPool().get_stream();
    auto mr = cudf_velox::get_output_mr();
    auto cudfTable = cudf_velox::with_arrow::toCudfTable(input, pool_.get(), stream, mr);
    auto evaluator = cudf_velox::createCudfExpression(expr, input->rowType(), pool_.get());

    auto ownedColumns = cudfTable->release();
    std::vector<cudf::column_view> inputViews;
    inputViews.reserve(ownedColumns.size());
    for (const auto& column : ownedColumns) {
      inputViews.push_back(column->view());
    }

    auto output = evaluator->eval(inputViews, stream, mr);
    cudf::table_view outputTable{{cudf_velox::asView(output)}};
    auto outputType = ROW({"result"}, {expr->type()});
    auto result = cudf_velox::with_arrow::toVeloxColumn(
        outputTable, pool_.get(), outputType, "", stream, cudf::get_current_device_resource_ref());
    stream.synchronize();
    result->setType(outputType);
    return result;
  }

  core::TypedExprPtr makeRowConstructorWithNull(const RowTypePtr& resultType) {
    return std::make_shared<core::CallTypedExpr>(
        resultType,
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
            std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c1")},
        RowConstructorWithNullCallToSpecialForm::kRowConstructorWithNull);
  }
};

TEST_F(GlutenCudfFunctionsTest, rowConstructorWithNull) {
  auto integers = makeNullableFlatVector<int32_t>({1, std::nullopt, 3, std::nullopt});
  auto strings = makeNullableFlatVector<std::string>({"a", "b", std::nullopt, std::nullopt});
  auto input = makeRowVector({"c0", "c1"}, {integers, strings});
  auto resultType = ROW({"left", "right"}, {INTEGER(), VARCHAR()});
  auto rowConstructor = makeRowConstructorWithNull(resultType);

  ASSERT_TRUE(cudf_velox::canExprRunOnGpu(rowConstructor, nullptr, pool_.get()));
  auto output = evaluateOnGpu(rowConstructor, input);
  auto result = output->childAt(0)->as<RowVector>();
  ASSERT_NE(result, nullptr);
  EXPECT_FALSE(result->isNullAt(0));
  EXPECT_TRUE(result->isNullAt(1));
  EXPECT_TRUE(result->isNullAt(2));
  EXPECT_TRUE(result->isNullAt(3));

  // cuDF structs require parent nulls to be superimposed on their children.
  EXPECT_TRUE(result->childAt(0)->isNullAt(2));
  EXPECT_TRUE(result->childAt(1)->isNullAt(1));

  auto firstField = std::make_shared<core::DereferenceTypedExpr>(INTEGER(), rowConstructor, 0);
  auto dereferenceOutput = evaluateOnGpu(firstField, input);
  auto actual = dereferenceOutput->childAt(0);
  auto expected = makeNullableFlatVector<int32_t>({1, std::nullopt, std::nullopt, std::nullopt});
  test::assertEqualVectors(expected, actual);
}

TEST_F(GlutenCudfFunctionsTest, rowConstructorWithNullLiteral) {
  auto integers = makeNullableFlatVector<int32_t>({1, std::nullopt, 3});
  auto input = makeRowVector({"c0"}, {integers});
  auto resultType = ROW({"left", "right"}, {INTEGER(), VARCHAR()});
  auto rowConstructor = std::make_shared<core::CallTypedExpr>(
      resultType,
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), variant::null(TypeKind::VARCHAR))},
      RowConstructorWithNullCallToSpecialForm::kRowConstructorWithNull);

  ASSERT_TRUE(cudf_velox::canExprRunOnGpu(rowConstructor, nullptr, pool_.get()));
  auto output = evaluateOnGpu(rowConstructor, input);
  auto result = output->childAt(0)->as<RowVector>();
  ASSERT_NE(result, nullptr);
  for (vector_size_t row = 0; row < result->size(); ++row) {
    EXPECT_TRUE(result->isNullAt(row));
    EXPECT_TRUE(result->childAt(0)->isNullAt(row));
    EXPECT_TRUE(result->childAt(1)->isNullAt(row));
  }
}

TEST_F(GlutenCudfFunctionsTest, rejectsAllLiteralRow) {
  auto resultType = ROW({"left", "right"}, {INTEGER(), VARCHAR()});
  auto rowConstructor = std::make_shared<core::CallTypedExpr>(
      resultType,
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::ConstantTypedExpr>(INTEGER(), variant(int32_t{1})),
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), variant("x"))},
      RowConstructorWithNullCallToSpecialForm::kRowConstructorWithNull);

  EXPECT_FALSE(cudf_velox::canExprRunOnGpu(rowConstructor, nullptr, pool_.get()));
  EXPECT_EQ(
      cudf_velox::createCudfFunction(
          RowConstructorWithNullCallToSpecialForm::kRowConstructorWithNull, rowConstructor, pool_.get()),
      nullptr);
}

TEST_F(GlutenCudfFunctionsTest, sparkLegacyCastIntegerToDouble) {
  auto input = makeRowVector(
      {"c0"},
      {makeNullableFlatVector<int32_t>(
          {std::numeric_limits<int32_t>::min(),
           -1,
           0,
           1,
           16777217,
           std::numeric_limits<int32_t>::max(),
           std::nullopt})});
  auto cast = std::make_shared<core::CallTypedExpr>(
      DOUBLE(),
      std::vector<core::TypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0")},
      "spark_legacy_cast");

  ASSERT_TRUE(cudf_velox::canExprRunOnGpu(cast, nullptr, pool_.get()));
  auto output = evaluateOnGpu(cast, input);
  auto expected =
      makeNullableFlatVector<double>({-2147483648.0, -1.0, 0.0, 1.0, 16777217.0, 2147483647.0, std::nullopt});
  test::assertEqualVectors(expected, output->childAt(0));

  auto nullInput = makeRowVector({"c0"}, {makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt})});
  auto nullOutput = evaluateOnGpu(cast, nullInput);
  test::assertEqualVectors(makeNullableFlatVector<double>({std::nullopt, std::nullopt}), nullOutput->childAt(0));

  auto emptyInput = makeRowVector({"c0"}, {makeFlatVector<int32_t>({})});
  auto emptyOutput = evaluateOnGpu(cast, emptyInput);
  test::assertEqualVectors(makeFlatVector<double>({}), emptyOutput->childAt(0));
}

TEST_F(GlutenCudfFunctionsTest, rejectsUnsupportedSparkLegacyCasts) {
  const std::vector<std::pair<TypePtr, TypePtr>> unsupported{
      {DATE(), DOUBLE()},
      {BIGINT(), DOUBLE()},
      {DECIMAL(10, 2), DOUBLE()},
      {DOUBLE(), INTEGER()},
      {VARCHAR(), INTEGER()},
      {INTEGER(), TINYINT()},
      {INTEGER(), REAL()}};
  for (const auto& [fromType, toType] : unsupported) {
    SCOPED_TRACE(fromType->toString() + " -> " + toType->toString());
    auto cast = std::make_shared<core::CallTypedExpr>(
        toType,
        std::vector<core::TypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(fromType, "c0")},
        "spark_legacy_cast");
    EXPECT_FALSE(cudf_velox::canExprRunOnGpu(cast, nullptr, pool_.get()));
    EXPECT_EQ(cudf_velox::createCudfFunction("spark_legacy_cast", cast, pool_.get()), nullptr);
  }
}

TEST_F(GlutenCudfFunctionsTest, rejectsLiteralSparkLegacyCast) {
  // The generic GPU cast expects an input column. Do not select it for a
  // literal-only call even if a caller bypasses Spark's constant folding.
  for (const auto& value : {variant(int32_t{1}), variant::null(TypeKind::INTEGER)}) {
    auto cast = std::make_shared<core::CallTypedExpr>(
        DOUBLE(),
        std::vector<core::TypedExprPtr>{std::make_shared<core::ConstantTypedExpr>(INTEGER(), value)},
        "spark_legacy_cast");
    EXPECT_FALSE(cudf_velox::canExprRunOnGpu(cast, nullptr, pool_.get()));
    EXPECT_EQ(cudf_velox::createCudfFunction("spark_legacy_cast", cast, pool_.get()), nullptr);
  }
}

} // namespace
} // namespace gluten
