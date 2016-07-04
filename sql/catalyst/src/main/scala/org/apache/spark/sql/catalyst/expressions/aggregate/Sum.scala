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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(x) - Returns the sum calculated from values of a group.")
case class Sum(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, DoubleType, DecimalType),
      ArrayType(LongType, false), ArrayType(DoubleType, false))

  override def checkInputDataTypes(): TypeCheckResult = {
    return child.dataType match {
      case ArrayType(LongType, _) => TypeCheckResult.TypeCheckSuccess
      case ArrayType(DoubleType, _) => TypeCheckResult.TypeCheckSuccess
      case _ => TypeUtils.checkForNumericExpr(child.dataType, "function sum")
    }
  }

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _ => child.dataType
  }

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType,
    !child.dataType.isInstanceOf[ArrayType] && child.nullable)()  // Gita's change

  private lazy val zero = Cast(Literal(0), sumDataType)

  override lazy val aggBufferAttributes = sum :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* sum = */
    if (!child.dataType.isInstanceOf[ArrayType] && child.nullable) {  // Gita's change
      Literal.create(null, sumDataType)
    } else {
      if (child.dataType.isInstanceOf[ArrayType]) {
        Literal.create(null, sumDataType)
      } else {
        zero
      }
    }
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (!child.dataType.isInstanceOf[ArrayType] && child.nullable) {
      Seq(
        /* sum = */
        Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType)), sum))
      )
    } else {
      Seq(
        /* sum = */
        if (child.dataType.isInstanceOf[ArrayType]) {
          InplaceAdd(sum, Cast(child, sumDataType))
        } else {
          Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType))
        }
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      if (child.dataType.isInstanceOf[ArrayType]) {
        Coalesce(Seq(InplaceAdd(sum.left, sum.right), sum.left))
      } else {
        Coalesce(Seq(Add(Coalesce(Seq(sum.left, zero)), sum.right), sum.left))
      }
    )
  }

  override lazy val evaluateExpression: Expression = sum
}
