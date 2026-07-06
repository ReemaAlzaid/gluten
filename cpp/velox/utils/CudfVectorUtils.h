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

#pragma once

#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"

#ifdef GLUTEN_ENABLE_GPU
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#endif

namespace gluten {

// Converts a GPU-resident CudfVector to a host RowVector. A CudfVector holds
// its data in a device-side cudf::table and exposes no host-side children, so
// any host code that reads childAt()/serializes it would see zero rows. When
// the input is a plain host RowVector it is returned unchanged.
inline facebook::velox::RowVectorPtr materializeVeloxRowVector(
    const facebook::velox::RowVectorPtr& rowVector,
    facebook::velox::memory::MemoryPool* memoryPool) {
#ifdef GLUTEN_ENABLE_GPU
  auto cudfVector = std::dynamic_pointer_cast<facebook::velox::cudf_velox::CudfVector>(rowVector);
  if (cudfVector != nullptr) {
    return facebook::velox::cudf_velox::with_arrow::toVeloxColumn(
        cudfVector->getTableView(),
        memoryPool,
        "",
        cudfVector->stream(),
        cudf::get_current_device_resource_ref());
  }
#endif
  return rowVector;
}

} // namespace gluten
