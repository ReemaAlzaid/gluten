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
package org.apache.gluten.extension

import org.scalatest.funsuite.AnyFunSuite

class CudfNodeValidationRuleSuite extends AnyFunSuite {

  // Records whether the (lazy) native validator was invoked, so we can assert it is NOT
  // called on paths that must short-circuit before touching the GPU.
  private class CountingValidator(result: Boolean) extends (() => Boolean) {
    var called: Boolean = false
    override def apply(): Boolean = {
      called = true
      result
    }
  }

  test("table-reading stage is rejected when GPU table scan is disabled, without validating") {
    val validator = new CountingValidator(true)
    val decision = CudfNodeValidationRule.decideOffload(
      hasLeaf = true,
      enableTableScan = false,
      enableValidation = true,
      validator)
    assert(!decision)
    assert(!validator.called, "native validator must not be called for a rejected scan stage")
  }

  test("table-reading stage is validated when GPU table scan is enabled") {
    // This is the regression guard: previously this path tagged GPU unconditionally and
    // skipped validation. Now an unsupported stage (validator -> false) must NOT be offloaded.
    val rejecting = new CountingValidator(false)
    assert(
      !CudfNodeValidationRule.decideOffload(
        hasLeaf = true,
        enableTableScan = true,
        enableValidation = true,
        rejecting))
    assert(rejecting.called, "native validator must run for a scan stage when table scan is on")

    // A supported scan stage (validator -> true) is still offloaded.
    assert(
      CudfNodeValidationRule.decideOffload(
        hasLeaf = true,
        enableTableScan = true,
        enableValidation = true,
        new CountingValidator(true)))
  }

  test("non-scan stage is offloaded only when validation passes") {
    assert(
      CudfNodeValidationRule.decideOffload(
        hasLeaf = false,
        enableTableScan = false,
        enableValidation = true,
        new CountingValidator(true)))
    assert(
      !CudfNodeValidationRule.decideOffload(
        hasLeaf = false,
        enableTableScan = false,
        enableValidation = true,
        new CountingValidator(false)))
  }

  test("validation disabled offloads optimistically without validating") {
    Seq(true, false).foreach {
      hasLeaf =>
        val validator = new CountingValidator(false)
        assert(
          CudfNodeValidationRule.decideOffload(
            hasLeaf = hasLeaf,
            enableTableScan = true,
            enableValidation = false,
            validator))
        assert(!validator.called, "native validator must not be called when validation is disabled")
    }
  }
}
