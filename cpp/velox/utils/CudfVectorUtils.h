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

#include <glog/logging.h>
#include <atomic>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"

#ifdef GLUTEN_ENABLE_GPU
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#endif

namespace gluten {

// Process-wide switch: when true, materializeVeloxRowVector() fails instead of
// converting, so tests/CI surface device-resident vectors escaping to host read
// sites. Set once at backend init from kCudfStrictResidency.
inline std::atomic<bool>& cudfStrictResidency() {
  static std::atomic<bool> strict{false};
  return strict;
}

// Converts a GPU-resident CudfVector to a host RowVector. A CudfVector holds
// its data in a device-side cudf::table and exposes no host-side children, so
// any host code that reads childAt()/serializes it would see zero rows. When
// the input is a plain host RowVector it is returned unchanged.
//
// A conversion here means a device vector reached a host read site without the
// cuDF driver adapter inserting a CudfToVelox in front of it, so each
// occurrence is logged; under strict residency it fails instead.
inline facebook::velox::RowVectorPtr materializeVeloxRowVector(
    const facebook::velox::RowVectorPtr& rowVector,
    facebook::velox::memory::MemoryPool* memoryPool) {
#ifdef GLUTEN_ENABLE_GPU
  auto cudfVector = std::dynamic_pointer_cast<facebook::velox::cudf_velox::CudfVector>(rowVector);
  if (cudfVector != nullptr) {
    VELOX_CHECK(
        !cudfStrictResidency().load(std::memory_order_relaxed),
        "Device-resident CudfVector reached a host read site while strict residency is enabled: {} rows",
        cudfVector->size());
    LOG_EVERY_N(WARNING, 100) << "Materializing device-resident CudfVector at a host read site (occurrence "
                              << google::COUNTER << ", " << cudfVector->size() << " rows).";
    return facebook::velox::cudf_velox::with_arrow::toVeloxColumn(
        cudfVector->getTableView(), memoryPool, "", cudfVector->stream(), cudf::get_current_device_resource_ref());
  }
#endif
  return rowVector;
}

} // namespace gluten
