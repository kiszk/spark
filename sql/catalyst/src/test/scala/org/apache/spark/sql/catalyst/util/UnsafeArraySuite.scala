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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.types.{DateType, Decimal, TimestampType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class UnsafeArraySuite extends SparkFunSuite {

  val booleanArray = Array(false, true)
  val shortArray = Array(1.toShort, 10.toShort, 100.toShort)
  val intArray = Array(1, 10, 100)
  val longArray = Array(1.toLong, 10.toLong, 100.toLong)
  val floatArray = Array(1.1.toFloat, 2.2.toFloat, 3.3.toFloat)
  val doubleArray = Array(1.1, 2.2, 3.3)
  val stringArray = Array("1", "10", "100")
  val dateArray = Array(
    DateTimeUtils.stringToDate(UTF8String.fromString("1970-1-1")).get,
    DateTimeUtils.stringToDate(UTF8String.fromString("2016-7-26")).get)
  val timestampArray = Array(
    DateTimeUtils.stringToTimestamp(UTF8String.fromString("1970-1-1 00:00:00")).get,
    DateTimeUtils.stringToTimestamp(UTF8String.fromString("2016-7-26 00:00:00")).get)
  val decimalArray = Array(Decimal(77L, 2, 1), Decimal(77L, 12, 1), Decimal(77L, 20, 1))
  val calenderintervalArray = Array(new CalendarInterval(3, 321), new CalendarInterval(1, 123))

  val intMultiDimArray = Array(Array(1), Array(2, 20), Array(3, 30, 300))
  val doubleMultiDimArray = Array(
    Array(1.1, 11.1), Array(2.2, 22.2, 222.2), Array(3.3, 33.3, 333.3, 3333.3))

  test("read array") {
    val unsafeBoolean = ExpressionEncoder[Array[Boolean]].resolveAndBind().
      toRow(booleanArray).getArray(0)
    assert(unsafeBoolean.isInstanceOf[UnsafeArrayData])
    assert(unsafeBoolean.numElements == booleanArray.length)
    booleanArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeBoolean.getBoolean(i) == e)
    }

    val unsafeShort = ExpressionEncoder[Array[Short]].resolveAndBind().
      toRow(shortArray).getArray(0)
    assert(unsafeShort.isInstanceOf[UnsafeArrayData])
    assert(unsafeShort.numElements == shortArray.length)
    shortArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeShort.getShort(i) == e)
    }

    val unsafeInt = ExpressionEncoder[Array[Int]].resolveAndBind().
      toRow(intArray).getArray(0)
    assert(unsafeInt.isInstanceOf[UnsafeArrayData])
    assert(unsafeInt.numElements == intArray.length)
    intArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeInt.getInt(i) == e)
    }

    val unsafeLong = ExpressionEncoder[Array[Long]].resolveAndBind().
      toRow(longArray).getArray(0)
    assert(unsafeLong.isInstanceOf[UnsafeArrayData])
    assert(unsafeLong.numElements == longArray.length)
    longArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeLong.getLong(i) == e)
    }

    val unsafeFloat = ExpressionEncoder[Array[Float]].resolveAndBind().
      toRow(floatArray).getArray(0)
    assert(unsafeFloat.isInstanceOf[UnsafeArrayData])
    assert(unsafeFloat.numElements == floatArray.length)
    floatArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeFloat.getFloat(i) == e)
    }

    val unsafeDouble = ExpressionEncoder[Array[Double]].resolveAndBind().
      toRow(doubleArray).getArray(0)
    assert(unsafeDouble.isInstanceOf[UnsafeArrayData])
    assert(unsafeDouble.numElements == doubleArray.length)
    doubleArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDouble.getDouble(i) == e)
    }

    val unsafeString = ExpressionEncoder[Array[String]].resolveAndBind().
      toRow(stringArray).getArray(0)
    assert(unsafeString.isInstanceOf[UnsafeArrayData])
    assert(unsafeString.numElements == stringArray.length)
    stringArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeString.getUTF8String(i).toString().equals(e))
    }

    val unsafeDate = ExpressionEncoder[Array[Int]].resolveAndBind().
      toRow(dateArray).getArray(0)
    assert(unsafeDate.isInstanceOf[UnsafeArrayData])
    assert(unsafeDate.numElements == dateArray.length)
    dateArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDate.get(i, DateType) == e)
    }

    val unsafeTimestamp = ExpressionEncoder[Array[Long]].resolveAndBind().
      toRow(timestampArray).getArray(0)
    assert(unsafeTimestamp.isInstanceOf[UnsafeArrayData])
    assert(unsafeTimestamp.numElements == timestampArray.length)
    timestampArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeTimestamp.get(i, TimestampType) == e)
    }

    val unsafeDecimal = ExpressionEncoder[Array[Decimal]].resolveAndBind().
      toRow(decimalArray).getArray(0)
    assert(unsafeDecimal.isInstanceOf[UnsafeArrayData])
    assert(unsafeDecimal.numElements == decimalArray.length)
    decimalArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDecimal.getDecimal(i, e.precision, e.scale) == e)
    }

    val unsafeInterval = ExpressionEncoder[Array[CalendarInterval]].resolveAndBind().
      toRow(calenderintervalArray).getArray(0)
    assert(unsafeInterval.isInstanceOf[UnsafeArrayData])
    assert(unsafeInterval.numElements == calenderintervalArray.length)
    calenderintervalArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeInterval.getInterval(i) == e)
    }

    val unsafeMultiDimInt = ExpressionEncoder[Array[Array[Int]]].resolveAndBind().
      toRow(intMultiDimArray).getArray(0)
    assert(unsafeMultiDimInt.isInstanceOf[UnsafeArrayData])
    assert(unsafeMultiDimInt.numElements == intMultiDimArray.length)
    intMultiDimArray.zipWithIndex.map { case (a, j) =>
      val u = unsafeMultiDimInt.getArray(j)
      assert(u.isInstanceOf[UnsafeArrayData])
      assert(u.numElements == a.length)
      a.zipWithIndex.map { case (e, i) =>
        assert(u.getInt(i) == e)
      }
    }

    val unsafeMultiDimDouble = ExpressionEncoder[Array[Array[Double]]].resolveAndBind().
      toRow(doubleMultiDimArray).getArray(0)
    assert(unsafeDouble.isInstanceOf[UnsafeArrayData])
    assert(unsafeMultiDimDouble.numElements == doubleMultiDimArray.length)
    doubleMultiDimArray.zipWithIndex.map { case (a, j) =>
      val u = unsafeMultiDimDouble.getArray(j)
      assert(u.isInstanceOf[UnsafeArrayData])
      assert(u.numElements == a.length)
      a.zipWithIndex.map { case (e, i) =>
        assert(u.getDouble(i) == e)
      }
    }
  }

  test("from primitive array") {
    val unsafeInt = UnsafeArrayData.fromPrimitiveArray(intArray)
    assert(unsafeInt.numElements == 3)
    assert(unsafeInt.getSizeInBytes ==
      ((8 + scala.math.ceil(3/64.toDouble) * 8 + 4 * 3 + 7).toInt / 8) * 8)
    intArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeInt.getInt(i) == e)
    }

    val unsafeDouble = UnsafeArrayData.fromPrimitiveArray(doubleArray)
    assert(unsafeDouble.numElements == 3)
    assert(unsafeDouble.getSizeInBytes ==
      ((8 + scala.math.ceil(3/64.toDouble) * 8 + 8 * 3 + 7).toInt / 8) * 8)
    doubleArray.zipWithIndex.map { case (e, i) =>
      assert(unsafeDouble.getDouble(i) == e)
    }
  }

  test("to primitive array") {
    val intEncoder = ExpressionEncoder[Array[Int]].resolveAndBind()
    assert(intEncoder.toRow(intArray).getArray(0).toIntArray.sameElements(intArray))

    val doubleEncoder = ExpressionEncoder[Array[Double]].resolveAndBind()
    assert(doubleEncoder.toRow(doubleArray).getArray(0).toDoubleArray.sameElements(doubleArray))
  }
}
