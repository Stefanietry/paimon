/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.catalyst.optimizer

import org.apache.paimon.spark.catalyst.plans.logical.LateralVectorSearch

import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/** Pushes filters on the query side below lateral vector search. */
object PushDownLateralVectorSearchFilter extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case Filter(condition, lvs: LateralVectorSearch) =>
      val predicates = splitConjunctivePredicates(condition)
      val (pushDown, stayUp) = predicates.partition {
        predicate => predicate.deterministic && predicate.references.subsetOf(lvs.child.outputSet)
      }

      if (pushDown.isEmpty) {
        Filter(condition, lvs)
      } else {
        val pushedChild = Filter(combineConjunctivePredicates(pushDown), lvs.child)
        val newLateralVectorSearch = lvs.copy(left = pushedChild)
        if (stayUp.isEmpty) {
          newLateralVectorSearch
        } else {
          Filter(combineConjunctivePredicates(stayUp), newLateralVectorSearch)
        }
      }
  }

  private def combineConjunctivePredicates(predicates: Seq[Expression]): Expression = {
    predicates.reduceLeft(And)
  }
}
