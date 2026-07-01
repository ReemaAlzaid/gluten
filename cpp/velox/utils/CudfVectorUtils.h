#pragma once

#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"

#ifdef GLUTEN_ENABLE_GPU
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#endif

namespace gluten {

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
