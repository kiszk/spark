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

package org.apache.spark.sql.catalyst.expressions

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

class ClosureToExpressionConverterSuite extends SparkFunSuite with ExpressionEvalHelper {

  def checkIfConversion[T: TypeTag](func: T => Boolean, expected: Expression): Unit = {
    assert(ClosureToExpressionConverter.convertFilter(func, ExpressionEncoder[T]().schema).get
      === Cast(If(expected, Literal(0), Literal(1)), BooleanType))
  }

  test("simple filter - Short") {
    checkIfConversion[Short](
      _ == 2,
      Not(EqualTo(UnresolvedAttribute("value"), Literal(2)))
    )
    checkIfConversion[Short](
      _ + 1 == 2,
      Not(EqualTo(Add(UnresolvedAttribute("value"), Literal(1)), Literal(2)))
    )
    checkIfConversion[Short](
      _ - 1 == 2,
      Not(EqualTo(Subtract(UnresolvedAttribute("value"), Literal(1)), Literal(2)))
    )
    checkIfConversion[Short](
      _ * 2 == 2,
      Not(EqualTo(Multiply(UnresolvedAttribute("value"), Literal(2)), Literal(2)))
    )
    checkIfConversion[Short](
      _ / 2 == 2,
      Not(EqualTo(Divide(UnresolvedAttribute("value"), Literal(2)), Literal(2)))
    )
    checkIfConversion[Short](
      s => (((s + 1) * 2 - 1) / 2) == 3,
      Not(EqualTo(Divide(Subtract(Multiply(Add(
          UnresolvedAttribute("value"), Literal(1)), Literal(2)), Literal(1)), Literal(2)),
        Literal(3)))
    )
    checkIfConversion[Short](
      _ > 3,
      LessThanOrEqual(UnresolvedAttribute("value"), Literal(3))
    )
    checkIfConversion[Short](
      s => Math.sqrt(s) + Math.log10(s) > 3,
      LessThanOrEqual(Subtract(
          Add(
            Sqrt(Cast(UnresolvedAttribute("value"), DoubleType)),
            Log10(Cast(UnresolvedAttribute("value"), DoubleType))
          ), Cast(Literal(3), DoubleType)),
        Literal(0))
    )
    checkIfConversion[Short](
      _ >= 3,
      LessThan(UnresolvedAttribute("value"), Literal(3))
    )
    checkIfConversion[Short](
      _ < 3,
      GreaterThanOrEqual(UnresolvedAttribute("value"), Literal(3))
    )
    checkIfConversion[Short](
      _ <= 3,
      GreaterThan(UnresolvedAttribute("value"), Literal(3))
    )
  }

  test("simple filter - Int") {
    checkIfConversion[Int](
      _ == 2,
      Not(EqualTo(UnresolvedAttribute("value"), Literal(2)))
    )
    checkIfConversion[Int](
      _ + 1 == 2,
      Not(EqualTo(Add(UnresolvedAttribute("value"), Literal(1)), Literal(2)))
    )
    checkIfConversion[Int](
      _ - 1 == 2,
      Not(EqualTo(Subtract(UnresolvedAttribute("value"), Literal(1)), Literal(2)))
    )
    checkIfConversion[Int](
      _ * 2 == 2,
      Not(EqualTo(Multiply(UnresolvedAttribute("value"), Literal(2)), Literal(2)))
    )
    checkIfConversion[Int](
      _ / 2 == 2,
      Not(EqualTo(Divide(UnresolvedAttribute("value"), Literal(2)), Literal(2)))
    )
    checkIfConversion[Int](
      s => (((s + 1) * 2 - 1) / 2) == 3,
      Not(EqualTo(Divide(Subtract(Multiply(Add(
          UnresolvedAttribute("value"), Literal(1)), Literal(2)), Literal(1)), Literal(2)),
        Literal(3)))
    )
    checkIfConversion[Int](
      _ > 3,
      LessThanOrEqual(UnresolvedAttribute("value"), Literal(3))
    )
    checkIfConversion[Int](
      s => Math.sqrt(s) + Math.log10(s) > 3,
      LessThanOrEqual(Subtract(
          Add(
            Sqrt(Cast(UnresolvedAttribute("value"), DoubleType)),
            Log10(Cast(UnresolvedAttribute("value"), DoubleType))
          ), Cast(Literal(3), DoubleType)),
        Literal(0))
    )
    checkIfConversion[Int](
      _ >= 3,
      LessThan(UnresolvedAttribute("value"), Literal(3))
    )
    checkIfConversion[Int](
      _ < 3,
      GreaterThanOrEqual(UnresolvedAttribute("value"), Literal(3))
    )
    checkIfConversion[Int](
      _ <= 3,
      GreaterThan(UnresolvedAttribute("value"), Literal(3))
    )
  }

  test("simple filter - Long") {
    checkIfConversion[Long](
      _ == 2,
      Not(EqualTo(Subtract(UnresolvedAttribute("value"), Literal(2L)), Literal(0)))
    )
    checkIfConversion[Long](
      _ + 1 == 2,
      Not(EqualTo(Subtract(
          Add(UnresolvedAttribute("value"), Literal(1L)), Literal(2L)),
        Literal(0)))
    )
    checkIfConversion[Long](
      _ - 1 == 2,
      Not(EqualTo(Subtract(
          Subtract(UnresolvedAttribute("value"), Literal(1L)), Literal(2L)),
        Literal(0)))
    )
    checkIfConversion[Long](
      _ * 2 == 2,
      Not(EqualTo(Subtract(
          Multiply(UnresolvedAttribute("value"), Literal(2L)), Literal(2L)),
        Literal(0)))
    )
    checkIfConversion[Long](
      _ / 2 == 2,
      Not(EqualTo(Subtract(
          Divide(UnresolvedAttribute("value"), Literal(2L)), Literal(2L)),
        Literal(0)))
    )
    checkIfConversion[Long](
      s => (((s + 1L) * 2L - 1L) / 2L) == 3L,
      Not(EqualTo(Subtract(
        Divide(Subtract(Multiply(Add(UnresolvedAttribute("value"),
          Literal(1L)), Literal(2L)), Literal(1L)), Literal(2L)),
            Literal(3L)),
        Literal(0)))
    )
    checkIfConversion[Long](
      _ > 3,
      LessThanOrEqual(Subtract(UnresolvedAttribute("value"), Literal(3L)),
        Literal(0))
    )
    checkIfConversion[Long](
      s => Math.sqrt(s) + Math.log10(s) > 3,
      LessThanOrEqual(Subtract(
          Add(
            Sqrt(Cast(UnresolvedAttribute("value"), DoubleType)),
            Log10(Cast(UnresolvedAttribute("value"), DoubleType))
          ), Cast(Literal(3), DoubleType)),
        Literal(0))
    )
    checkIfConversion[Long](
      _ >= 3,
      LessThan(Subtract(UnresolvedAttribute("value"), Literal(3L)),
        Literal(0))
    )
    checkIfConversion[Long](
      _ < 3,
      GreaterThanOrEqual(Subtract(UnresolvedAttribute("value"), Literal(3L)),
        Literal(0))
    )
    checkIfConversion[Long](
      _ <= 3,
      GreaterThan(Subtract(UnresolvedAttribute("value"), Literal(3L)),
        Literal(0))
    )
  }

  test("simple filter - Float") {
    checkIfConversion[Float](
      _ == 2,
      Not(EqualTo(Subtract(UnresolvedAttribute("value"), Cast(Literal(2), FloatType)),
        Literal(0)))
    )
    checkIfConversion[Float](
      _ + 1 == 2,
      Not(EqualTo(Subtract(
            Add(UnresolvedAttribute("value"), Cast(Literal(1), FloatType)),
          Cast(Literal(2), FloatType)),
        Literal(0)))
    )
    checkIfConversion[Float](
      _ - 1 == 2,
      Not(EqualTo(Subtract(
            Subtract(UnresolvedAttribute("value"), Cast(Literal(1), FloatType)),
          Cast(Literal(2), FloatType)),
        Literal(0)))
    )
    checkIfConversion[Float](
      _ * 2 == 2,
      Not(EqualTo(Subtract(
            Multiply(UnresolvedAttribute("value"), Cast(Literal(2), FloatType)),
          Cast(Literal(2), FloatType)),
        Literal(0)))
    )
    checkIfConversion[Float](
      _ / 2 == 2,
      Not(EqualTo(Subtract(
            Divide(UnresolvedAttribute("value"), Cast(Literal(2), FloatType)),
          Cast(Literal(2), FloatType)),
        Literal(0)))
    )
    checkIfConversion[Float](
      _ > 3,
      LessThanOrEqual(Subtract(UnresolvedAttribute("value"), Cast(Literal(3), FloatType)),
        Literal(0))
    )
    checkIfConversion[Float](
      s => (((s + 1.0f) * 2.0f - 1.0f) / 2.0f) > 3.0f,
      LessThanOrEqual(Subtract(
        Divide(Subtract(Multiply(Add(UnresolvedAttribute("value"),
          Literal(1.0f)), Literal(2.0f)), Literal(1.0f)), Literal(2.0f)), Literal(3.0f)),
        Literal(0))
    )
    checkIfConversion[Float](
      s => Math.sqrt(s) + Math.log10(s) > 3,
      LessThanOrEqual(Subtract(
          Add(
            Sqrt(Cast(UnresolvedAttribute("value"), DoubleType)),
            Log10(Cast(UnresolvedAttribute("value"), DoubleType))
          ), Cast(Literal(3), DoubleType)),
        Literal(0))
    )
    checkIfConversion[Float](
      _ >= 3,
      LessThan(Subtract(UnresolvedAttribute("value"), Cast(Literal(3), FloatType)),
        Literal(0))
    )
    checkIfConversion[Float](
      _ < 3,
      GreaterThanOrEqual(Subtract(Cast(Literal(3), FloatType), UnresolvedAttribute("value")),
        Literal(0))
    )
    checkIfConversion[Float](
      _ <= 3,
      GreaterThan(Subtract(Cast(Literal(3), FloatType), UnresolvedAttribute("value")),
        Literal(0))
    )
  }

  test("simple filter - Double") {
    checkIfConversion[Double](
      _ == 2,
      Not(EqualTo(Subtract(UnresolvedAttribute("value"), Cast(Literal(2), DoubleType)),
        Literal(0)))
    )
    checkIfConversion[Double](
      _ + 1 == 2,
      Not(EqualTo(Subtract(
            Add(UnresolvedAttribute("value"), Cast(Literal(1), DoubleType)),
          Cast(Literal(2), DoubleType)),
        Literal(0)))
    )
    checkIfConversion[Double](
      _ - 1 == 2,
      Not(EqualTo(Subtract(
            Subtract(UnresolvedAttribute("value"), Cast(Literal(1), DoubleType)),
          Cast(Literal(2), DoubleType)),
        Literal(0)))
    )
    checkIfConversion[Double](
      _ * 2 == 2,
      Not(EqualTo(Subtract(
            Multiply(UnresolvedAttribute("value"), Cast(Literal(2), DoubleType)),
          Cast(Literal(2), DoubleType)),
        Literal(0)))
    )
    checkIfConversion[Double](
      _ / 2 == 2,
      Not(EqualTo(Subtract(
            Divide(UnresolvedAttribute("value"), Cast(Literal(2), DoubleType)),
          Cast(Literal(2), DoubleType)),
        Literal(0)))
    )
    checkIfConversion[Double](
      _ > 3,
      LessThanOrEqual(Subtract(UnresolvedAttribute("value"), Cast(Literal(3), DoubleType)),
        Literal(0))
    )
    checkIfConversion[Double](
      s => (((s + 1.0) * 2.0 - 1.0) / 2.0) > 3.0,
      LessThanOrEqual(Subtract(
        Divide(Subtract(Multiply(Add(UnresolvedAttribute("value"),
          Literal(1.0)), Literal(2.0)), Literal(1.0)), Literal(2.0)), Literal(3.0)),
        Literal(0))
    )
    checkIfConversion[Double](
      s => Math.sqrt(s) + Math.log10(s) > 3,
      LessThanOrEqual(Subtract(
            Add(Sqrt(UnresolvedAttribute("value")), Log10(UnresolvedAttribute("value"))),
          Cast(Literal(3), DoubleType)),
        Literal(0))
    )
    checkIfConversion[Double](
      _ >= 3,
      LessThan(Subtract(UnresolvedAttribute("value"), Cast(Literal(3), DoubleType)),
        Literal(0))
    )
    checkIfConversion[Double](
      _ < 3,
      GreaterThanOrEqual(Subtract(Cast(Literal(3), DoubleType), UnresolvedAttribute("value")),
        Literal(0))
    )
    checkIfConversion[Double](
      _ <= 3,
      GreaterThan(Subtract(Cast(Literal(3), DoubleType), UnresolvedAttribute("value")),
        Literal(0))
    )
  }

  test("simple filter - String") {
    checkIfConversion[String](
      _.length > 3,
      LessThanOrEqual(Length(UnresolvedAttribute("value")), Literal(3))
    )
  }

  test("simple filter - Tuple") {
    checkIfConversion[Tuple2[Int, Int]](
      t => t._1 + t._2 > 4,
      LessThanOrEqual(Add(UnresolvedAttribute("_1"), UnresolvedAttribute("_2")), Literal(4))
    )
    checkIfConversion[Tuple3[Int, Int, String]](
      t => t._3.length > 2,
      LessThanOrEqual(
          Length(UnresolvedAttribute("_3")),
        Literal(2))
    )
    checkIfConversion[Tuple4[Int, Int, Int, Double]](
      t => Math.sqrt(t._4) > 3.0,
      LessThanOrEqual(Subtract(
          Sqrt(
            If(IsNotNull(UnresolvedAttribute("_4")),
              UnresolvedAttribute("_4"),
              Literal(0.0))),
          Literal(3.0)),
        Literal(0))
    )
  }

  def checkMapConversion[T: TypeTag, U: TypeTag](func: T => U, expected: Expression): Unit = {
    assert(ClosureToExpressionConverter.convertMap(func, ExpressionEncoder[T]().schema).get
      === expected)
  }

  test("simple map") {
    checkMapConversion[Short, Int](
      s => (((s + 1) * 2 - 1) / 2),
      Divide(Subtract(Multiply(Add(UnresolvedAttribute("value"), Literal(1)), Literal(2)),
        Literal(1)), Literal(2))
    )
    checkMapConversion[Int, Boolean](
      s => s + 1 > 4,
      If(LessThanOrEqual(Add(UnresolvedAttribute("value"), Literal(1)), Literal(4)),
        Literal(0), Literal(1))
    )
    checkMapConversion[Double, Double](
      s => Math.sqrt(s) + Math.log10(s),
      Add(Sqrt(UnresolvedAttribute("value")), Log10(UnresolvedAttribute("value")))
    )
    checkMapConversion[String, Int](
      s => s.length,
      Length(UnresolvedAttribute("value"))
    )
    checkMapConversion[Tuple2[Int, Int], Int](
      t => t._1 + t._2,
      Add(UnresolvedAttribute("_1"), UnresolvedAttribute("_2"))
    )
  }

  val personSchema = StructType(Seq(
    StructField("firstName", StringType),
    StructField("lastName", StringType),
    StructField("age", IntegerType)
  ))

  test("equality filter on int column") {
    val expr = ClosureToExpressionConverter.convertFilter(
      (row: Row) => row.getInt(2) == 4,
      personSchema).get
    val expectedExpr = Cast(
      If(
        Not(EqualTo(NPEOnNull(UnresolvedAttribute("age")), Literal(4))),
        Literal(0),
        Literal(1)),
      BooleanType)
    assert(expr === expectedExpr)
  }
}
