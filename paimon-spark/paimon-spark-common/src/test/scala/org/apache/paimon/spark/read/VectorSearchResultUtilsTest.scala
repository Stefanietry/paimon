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

package org.apache.paimon.spark.read

import org.apache.paimon.globalindex.{GlobalIndexResult, ScoredGlobalIndexResult}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.utils.RoaringNavigableMap64

import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** Tests for [[VectorSearchResultUtils]]. */
class VectorSearchResultUtilsTest extends AnyFunSuite {

  test("identify row id and score only fields") {
    assert(VectorSearchResultUtils.isRowIdScoreOnly(Seq(PaimonMetadataColumn.ROW_ID_COLUMN)))
    assert(VectorSearchResultUtils.isRowIdScoreOnly(Seq(
      PaimonMetadataColumn.SEARCH_SCORE_COLUMN,
      PaimonMetadataColumn.ROW_ID_COLUMN)))
    assert(!VectorSearchResultUtils.isRowIdScoreOnly(Seq.empty))
    assert(!VectorSearchResultUtils.isRowIdScoreOnly(Seq(
      PaimonMetadataColumn.ROW_ID_COLUMN,
      "id")))
  }

  test("convert scored global index result to row id and score rows") {
    val bitmap = new RoaringNavigableMap64()
    bitmap.add(1L)
    bitmap.add(3L)
    val result = ScoredGlobalIndexResult.create(bitmap, rowId => rowId.toFloat + 0.5f)
    val schema = StructType(Seq(
      StructField(PaimonMetadataColumn.ROW_ID_COLUMN, LongType),
      StructField(PaimonMetadataColumn.SEARCH_SCORE_COLUMN, FloatType)))

    val rows = VectorSearchResultUtils.toRows(result, schema)

    assert(rows.length == 2)
    assert(rows(0).getLong(0) == 1L)
    assert(rows(0).getFloat(1) == 1.5f)
    assert(rows(1).getLong(0) == 3L)
    assert(rows(1).getFloat(1) == 3.5f)
  }

  test("use NaN score for unscored global index result") {
    val bitmap = new RoaringNavigableMap64()
    bitmap.add(2L)
    val schema = StructType(Seq(StructField(PaimonMetadataColumn.SEARCH_SCORE_COLUMN, FloatType)))

    val rows = VectorSearchResultUtils.toRows(GlobalIndexResult.create(bitmap), schema)

    assert(rows.length == 1)
    assert(rows(0).getFloat(0).isNaN)
  }
}
