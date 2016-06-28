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

package org.apache.spark.sql

import org.apache.spark.sql.execution.debug.codegenString
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

/**
 * A test suite to test DataFrame/SQL functionalities with complex types (i.e. array, struct, map).
 */
class DataFrameComplexTypeSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  def validate(df: DataFrame): Unit = {
    val logicalPlan = df.logicalPlan
    val queryExecution = sqlContext.sessionState.executePlan(logicalPlan)
    val cg = codegenString(queryExecution.executedPlan)

    if (cg.contains("Found 0 WholeStageCodegen subtrees")) {
      return
    }

    if ("zeroOutNullBytes".r.findFirstIn(cg).isDefined) {
      fail(
        s"""
       |=== FAIL: generated code must not include: zeroOutNullBytes ===
       |$cg
       """.stripMargin
      )
    }
  }

  test ("check elimination of zeroOutNullBytes on array") {
    val df = sparkContext.parallelize(Seq(1, 2), 1).toDF("v")
    validate(df.selectExpr("Array(v + 3, v + 4)"))
  }

  test ("check elimination of zeroOutNullBytes on map") {
    val df = sparkContext.parallelize(Seq(1, 2), 1).toDF("v")
    validate(df.selectExpr("struct(v + 3, v + 4)"))
  }

  test ("check elimination of zeroOutNullBytes on struct") {
    val df = sparkContext.parallelize(Seq(1, 2), 1).toDF("v")
    validate(df.selectExpr("Array(v + 3, v + 4)"))
  }

  test("UDF on struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(struct($"a").as("s")).select(f($"s.a")).collect()
  }

  test("UDF on named_struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.selectExpr("named_struct('a', a) s").select(f($"s.a")).collect()
  }

  test("UDF on array") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(array($"a").as("s")).select(f($"s".getItem(0))).collect()
  }

  test("UDF on map") {
    val f = udf((a: String) => a)
    val df = Seq("a" -> 1).toDF("a", "b")
    df.select(map($"a", $"b").as("s")).select(f($"s".getItem("a"))).collect()
  }

  test("SPARK-12477 accessing null element in array field") {
    val df = sparkContext.parallelize(Seq((Seq("val1", null, "val2"),
      Seq(Some(1), None, Some(2))))).toDF("s", "i")
    val nullStringRow = df.selectExpr("s[1]").collect()(0)
    assert(nullStringRow == org.apache.spark.sql.Row(null))
    val nullIntRow = df.selectExpr("i[1]").collect()(0)
    assert(nullIntRow == org.apache.spark.sql.Row(null))
  }
}
