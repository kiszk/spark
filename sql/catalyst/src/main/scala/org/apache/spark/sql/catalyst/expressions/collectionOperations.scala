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

import java.util.Comparator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.ArraySortUtil.NullOrder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Given an array or map, returns its size. Returns -1 if null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the size of an array or a map. Returns -1 if null.",
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', 'c', 'a'));
       4
  """)
case class Size(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(ArrayType, MapType))
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      -1
    } else child.dataType match {
      case _: ArrayType => value.asInstanceOf[ArrayData].numElements()
      case _: MapType => value.asInstanceOf[MapData].numElements()
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    ev.copy(code = s"""
      boolean ${ev.isNull} = false;
      ${childGen.code}
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${childGen.isNull} ? -1 :
        (${childGen.value}).numElements();""", isNull = FalseLiteral)
  }
}

/**
 * Returns an unordered array containing the keys of the map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map) - Returns an unordered array containing the keys of the map.",
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'));
       [1,2]
  """)
case class MapKeys(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].keyType)

  override def nullSafeEval(map: Any): Any = {
    map.asInstanceOf[MapData].keyArray()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"${ev.value} = ($c).keyArray();")
  }

  override def prettyName: String = "map_keys"
}

/**
 * Returns an unordered array containing the values of the map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map) - Returns an unordered array containing the values of the map.",
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'));
       ["a","b"]
  """)
case class MapValues(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].valueType)

  override def nullSafeEval(map: Any): Any = {
    map.asInstanceOf[MapData].valueArray()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"${ev.value} = ($c).valueArray();")
  }

  override def prettyName: String = "map_values"
}

/**
 * Common base class for [[SortArray]] and [[ArraySort]].
 */
trait ArraySortUtil extends ExpectsInputTypes {
  protected def arrayExpression: Expression

  protected def nullOrder: NullOrder

  @transient
  private lazy val lt: Comparator[Any] = {
    val ordering = arrayExpression.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          nullOrder
        } else if (o2 == null) {
          -nullOrder
        } else {
          ordering.compare(o1, o2)
        }
      }
    }
  }

  @transient
  private lazy val gt: Comparator[Any] = {
    val ordering = arrayExpression.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          -nullOrder
        } else if (o2 == null) {
          nullOrder
        } else {
          -ordering.compare(o1, o2)
        }
      }
    }
  }

  def elementType: DataType = arrayExpression.dataType.asInstanceOf[ArrayType].elementType

  def sortEval(array: Any, ascending: Boolean): Any = {
    val data = array.asInstanceOf[ArrayData].toArray[AnyRef](elementType)
    if (elementType != NullType) {
      java.util.Arrays.sort(data, if (ascending) lt else gt)
    }
    new GenericArrayData(data.asInstanceOf[Array[Any]])
  }

  def sortCodegen(ctx: CodegenContext, ev: ExprCode, base: String, order: String): String = {
    val arrayData = classOf[ArrayData].getName
    val genericArrayData = classOf[GenericArrayData].getName
    val array = ctx.freshName("array")
    val c = ctx.freshName("c")
    val sort = if (elementType == NullType) "" else {
      val sortOrder = ctx.freshName("sortOrder")
      val o1 = ctx.freshName("o1")
      val o2 = ctx.freshName("o2")
      val jt = CodeGenerator.javaType(elementType)
      val comp = if (CodeGenerator.isPrimitiveType(elementType)) {
        val bt = CodeGenerator.boxedType(elementType)
        val v1 = ctx.freshName("v1")
        val v2 = ctx.freshName("v2")
        s"""
           |$jt $v1 = (($bt) $o1).${jt}Value();
           |$jt $v2 = (($bt) $o2).${jt}Value();
           |int $c = ${ctx.genComp(elementType, v1, v2)};
         """.stripMargin
      } else {
        s"int $c = ${ctx.genComp(elementType, s"(($jt) $o1)", s"(($jt) $o2)")};"
      }
      s"""
         |final int $sortOrder = $order ? 1 : -1;
         |java.util.Arrays.sort($array, new java.util.Comparator() {
         |  @Override public int compare(Object $o1, Object $o2) {
         |    if ($o1 == null && $o2 == null) {
         |      return 0;
         |    } else if ($o1 == null) {
         |      return $sortOrder * $nullOrder;
         |    } else if ($o2 == null) {
         |      return -$sortOrder * $nullOrder;
         |    }
         |    $comp
         |    return $sortOrder * $c;
         |  }
         |});
       """.stripMargin
    }
    val dataTypes = elementType match {
      case DecimalType.Fixed(p, s) =>
        s"org.apache.spark.sql.types.DataTypes.createDecimalType($p, $s)"
      case ArrayType(et, cn) =>
        s"org.apache.spark.sql.types.DataTypes.createArrayType($et, $cn)"
      case MapType(kt, vt, cn) =>
        s"org.apache.spark.sql.types.DataTypes.createMapType($kt, $vt, $cn)"
      case StructType(f) =>
        "org.apache.spark.sql.types.StructType$.MODULE$." +
          s"apply(new java.util.ArrayList(${f.length}))"
      case _ =>
        s"org.apache.spark.sql.types.DataTypes.$elementType"
    }
    s"""
       |Object[] $array = (Object[]) (($arrayData) $base).toArray(
       |  $dataTypes, scala.reflect.ClassTag$$.MODULE$$.AnyRef());
       |$sort
       |${ev.value} = new $genericArrayData($array);
     """.stripMargin
  }

}

object ArraySortUtil {
  type NullOrder = Int
  // Least: place null element at the first of the array for ascending order
  // Greatest: place null element at the end of the array for ascending order
  object NullOrder {
    val Least: NullOrder = -1
    val Greatest: NullOrder = 1
  }
}

/**
 * Sorts the input array in ascending / descending order according to the natural ordering of
 * the array elements and returns it.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array[, ascendingOrder]) - Sorts the input array in ascending or descending order
      according to the natural ordering of the array elements. Null elements will be placed
      at the beginning of the returned array in ascending order or at the end of the returned
      array in descending order.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', null, 'c', 'a'), true);
       [null,"a","b","c","d"]
  """)
// scalastyle:on line.size.limit
case class SortArray(base: Expression, ascendingOrder: Expression)
  extends BinaryExpression with ArraySortUtil {

  def this(e: Expression) = this(e, Literal(true))

  override def left: Expression = base
  override def right: Expression = ascendingOrder
  override def dataType: DataType = base.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def arrayExpression: Expression = base
  override def nullOrder: NullOrder = NullOrder.Least

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      ascendingOrder match {
        case Literal(_: Boolean, BooleanType) =>
          TypeCheckResult.TypeCheckSuccess
        case _ =>
          TypeCheckResult.TypeCheckFailure(
            "Sort order in second argument requires a boolean literal.")
      }
    case ArrayType(dt, _) =>
      val dtSimple = dt.simpleString
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type $dtSimple which is not orderable")
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input.")
  }

  override def nullSafeEval(array: Any, ascending: Any): Any = {
    sortEval(array, ascending.asInstanceOf[Boolean])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (b, order) => sortCodegen(ctx, ev, b, order))
  }

  override def prettyName: String = "sort_array"
}


/**
 * Sorts the input array in ascending order according to the natural ordering of
 * the array elements and returns it.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Sorts the input array in ascending order. The elements of the input array must
      be orderable. Null elements will be placed at the end of the returned array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', null, 'c', 'a'));
       ["a","b","c","d",null]
  """,
  since = "2.4.0")
// scalastyle:on line.size.limit
case class ArraySort(child: Expression) extends UnaryExpression with ArraySortUtil {

  override def dataType: DataType = child.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  override def arrayExpression: Expression = child
  override def nullOrder: NullOrder = NullOrder.Greatest

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      TypeCheckResult.TypeCheckSuccess
    case ArrayType(dt, _) =>
      val dtSimple = dt.simpleString
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type $dtSimple which is not orderable")
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input.")
  }

  override def nullSafeEval(array: Any): Any = {
    sortEval(array, true)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => sortCodegen(ctx, ev, c, "true"))
  }

  override def prettyName: String = "array_sort"
}

/**
 * Returns a reversed string or an array with reverse order of elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Returns a reversed string or an array with reverse order of elements.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL');
       LQS krapS
      > SELECT _FUNC_(array(2, 1, 4, 3));
       [3, 4, 1, 2]
  """,
  since = "1.5.0",
  note = "Reverse logic for arrays is available since 2.4.0."
)
case class Reverse(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  // Input types are utilized by type coercion in ImplicitTypeCasts.
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, ArrayType))

  override def dataType: DataType = child.dataType

  lazy val elementType: DataType = dataType.asInstanceOf[ArrayType].elementType

  override def nullSafeEval(input: Any): Any = input match {
    case a: ArrayData => new GenericArrayData(a.toObjectArray(elementType).reverse)
    case s: UTF8String => s.reverse()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => dataType match {
      case _: StringType => stringCodeGen(ev, c)
      case _: ArrayType => arrayCodeGen(ctx, ev, c)
    })
  }

  private def stringCodeGen(ev: ExprCode, childName: String): String = {
    s"${ev.value} = ($childName).reverse();"
  }

  private def arrayCodeGen(ctx: CodegenContext, ev: ExprCode, childName: String): String = {
    val length = ctx.freshName("length")
    val javaElementType = CodeGenerator.javaType(elementType)
    val isPrimitiveType = CodeGenerator.isPrimitiveType(elementType)

    val initialization = if (isPrimitiveType) {
      s"$childName.copy()"
    } else {
      s"new ${classOf[GenericArrayData].getName()}(new Object[$length])"
    }

    val numberOfIterations = if (isPrimitiveType) s"$length / 2" else length

    val swapAssigments = if (isPrimitiveType) {
      val setFunc = "set" + CodeGenerator.primitiveTypeName(elementType)
      val getCall = (index: String) => CodeGenerator.getValue(ev.value, elementType, index)
      s"""|boolean isNullAtK = ${ev.value}.isNullAt(k);
          |boolean isNullAtL = ${ev.value}.isNullAt(l);
          |if(!isNullAtK) {
          |  $javaElementType el = ${getCall("k")};
          |  if(!isNullAtL) {
          |    ${ev.value}.$setFunc(k, ${getCall("l")});
          |  } else {
          |    ${ev.value}.setNullAt(k);
          |  }
          |  ${ev.value}.$setFunc(l, el);
          |} else if (!isNullAtL) {
          |  ${ev.value}.$setFunc(k, ${getCall("l")});
          |  ${ev.value}.setNullAt(l);
          |}""".stripMargin
    } else {
      s"${ev.value}.update(k, ${CodeGenerator.getValue(childName, elementType, "l")});"
    }

    s"""
       |final int $length = $childName.numElements();
       |${ev.value} = $initialization;
       |for(int k = 0; k < $numberOfIterations; k++) {
       |  int l = $length - k - 1;
       |  $swapAssigments
       |}
     """.stripMargin
  }

  override def prettyName: String = "reverse"
}

/**
 * Checks if the array (left) has the element (right)
 */
@ExpressionDescription(
  usage = "_FUNC_(array, value) - Returns true if the array contains the value.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 2);
       true
  """)
case class ArrayContains(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = right.dataType match {
    case NullType => Seq.empty
    case _ => left.dataType match {
      case n @ ArrayType(element, _) => Seq(n, element)
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (right.dataType == NullType) {
      TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as arguments")
    } else if (!left.dataType.isInstanceOf[ArrayType]
      || left.dataType.asInstanceOf[ArrayType].elementType != right.dataType) {
      TypeCheckResult.TypeCheckFailure(
        "Arguments must be an array followed by a value of same type as the array members")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull
  }

  override def nullSafeEval(arr: Any, value: Any): Any = {
    var hasNull = false
    arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) =>
      if (v == null) {
        hasNull = true
      } else if (v == value) {
        return true
      }
    )
    if (hasNull) {
      null
    } else {
      false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (arr, value) => {
      val i = ctx.freshName("i")
      val getValue = CodeGenerator.getValue(arr, right.dataType, i)
      s"""
      for (int $i = 0; $i < $arr.numElements(); $i ++) {
        if ($arr.isNullAt($i)) {
          ${ev.isNull} = true;
        } else if (${ctx.genEqual(right.dataType, value, getValue)}) {
          ${ev.isNull} = false;
          ${ev.value} = true;
          break;
        }
      }
     """
    })
  }

  override def prettyName: String = "array_contains"
}

/**
 * Returns the minimum value in the array.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Returns the minimum value in the array. NULL elements are skipped.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 20, null, 3));
       1
  """, since = "2.4.0")
case class ArrayMin(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = super.checkInputDataTypes()
    if (typeCheckResult.isSuccess) {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    } else {
      typeCheckResult
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val i = ctx.freshName("i")
    val item = ExprCode("",
      isNull = JavaCode.isNullExpression(s"${childGen.value}.isNullAt($i)"),
      value = JavaCode.expression(CodeGenerator.getValue(childGen.value, dataType, i), dataType))
    ev.copy(code =
      s"""
         |${childGen.code}
         |boolean ${ev.isNull} = true;
         |$javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${childGen.isNull}) {
         |  for (int $i = 0; $i < ${childGen.value}.numElements(); $i ++) {
         |    ${ctx.reassignIfSmaller(dataType, ev, item)}
         |  }
         |}
      """.stripMargin)
  }

  override protected def nullSafeEval(input: Any): Any = {
    var min: Any = null
    input.asInstanceOf[ArrayData].foreach(dataType, (_, item) =>
      if (item != null && (min == null || ordering.lt(item, min))) {
        min = item
      }
    )
    min
  }

  override def dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_min"
}

/**
 * Returns the maximum value in the array.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Returns the maximum value in the array. NULL elements are skipped.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 20, null, 3));
       20
  """, since = "2.4.0")
case class ArrayMax(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = super.checkInputDataTypes()
    if (typeCheckResult.isSuccess) {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    } else {
      typeCheckResult
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val i = ctx.freshName("i")
    val item = ExprCode("",
      isNull = JavaCode.isNullExpression(s"${childGen.value}.isNullAt($i)"),
      value = JavaCode.expression(CodeGenerator.getValue(childGen.value, dataType, i), dataType))
    ev.copy(code =
      s"""
         |${childGen.code}
         |boolean ${ev.isNull} = true;
         |$javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${childGen.isNull}) {
         |  for (int $i = 0; $i < ${childGen.value}.numElements(); $i ++) {
         |    ${ctx.reassignIfGreater(dataType, ev, item)}
         |  }
         |}
      """.stripMargin)
  }

  override protected def nullSafeEval(input: Any): Any = {
    var max: Any = null
    input.asInstanceOf[ArrayData].foreach(dataType, (_, item) =>
      if (item != null && (max == null || ordering.gt(item, max))) {
        max = item
      }
    )
    max
  }

  override def dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_max"
}


/**
 * Returns the position of the first occurrence of element in the given array as long.
 * Returns 0 if the given value could not be found in the array. Returns null if either of
 * the arguments are null
 *
 * NOTE: that this is not zero based, but 1-based index. The first element in the array has
 *       index 1.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, element) - Returns the (1-based) index of the first element of the array as long.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(3, 2, 1), 1);
       3
  """,
  since = "2.4.0")
case class ArrayPosition(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = LongType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(ArrayType, left.dataType.asInstanceOf[ArrayType].elementType)

  override def nullSafeEval(arr: Any, value: Any): Any = {
    arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) =>
      if (v == value) {
        return (i + 1).toLong
      }
    )
    0L
  }

  override def prettyName: String = "array_position"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (arr, value) => {
      val pos = ctx.freshName("arrayPosition")
      val i = ctx.freshName("i")
      val getValue = CodeGenerator.getValue(arr, right.dataType, i)
      s"""
         |int $pos = 0;
         |for (int $i = 0; $i < $arr.numElements(); $i ++) {
         |  if (!$arr.isNullAt($i) && ${ctx.genEqual(right.dataType, value, getValue)}) {
         |    $pos = $i + 1;
         |    break;
         |  }
         |}
         |${ev.value} = (long) $pos;
       """.stripMargin
    })
  }
}

/**
 * Returns the value of index `right` in Array `left` or the value for key `right` in Map `left`.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, index) - Returns element of array at given (1-based) index. If index < 0,
      accesses elements from the last to the first. Returns NULL if the index exceeds the length
      of the array.

    _FUNC_(map, key) - Returns value for given key, or NULL if the key is not contained in the map
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 2);
       2
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), 2);
       "b"
  """,
  since = "2.4.0")
case class ElementAt(left: Expression, right: Expression) extends GetMapValueUtil {

  override def dataType: DataType = left.dataType match {
    case ArrayType(elementType, _) => elementType
    case MapType(_, valueType, _) => valueType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(ArrayType, MapType),
      left.dataType match {
        case _: ArrayType => IntegerType
        case _: MapType => left.dataType.asInstanceOf[MapType].keyType
      }
    )
  }

  override def nullable: Boolean = true

  override def nullSafeEval(value: Any, ordinal: Any): Any = {
    left.dataType match {
      case _: ArrayType =>
        val array = value.asInstanceOf[ArrayData]
        val index = ordinal.asInstanceOf[Int]
        if (array.numElements() < math.abs(index)) {
          null
        } else {
          val idx = if (index == 0) {
            throw new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
          } else if (index > 0) {
            index - 1
          } else {
            array.numElements() + index
          }
          if (left.dataType.asInstanceOf[ArrayType].containsNull && array.isNullAt(idx)) {
            null
          } else {
            array.get(idx, dataType)
          }
        }
      case _: MapType =>
        getValueEval(value, ordinal, left.dataType.asInstanceOf[MapType].keyType)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    left.dataType match {
      case _: ArrayType =>
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          val index = ctx.freshName("elementAtIndex")
          val nullCheck = if (left.dataType.asInstanceOf[ArrayType].containsNull) {
            s"""
               |if ($eval1.isNullAt($index)) {
               |  ${ev.isNull} = true;
               |} else
             """.stripMargin
          } else {
            ""
          }
          s"""
             |int $index = (int) $eval2;
             |if ($eval1.numElements() < Math.abs($index)) {
             |  ${ev.isNull} = true;
             |} else {
             |  if ($index == 0) {
             |    throw new ArrayIndexOutOfBoundsException("SQL array indices start at 1");
             |  } else if ($index > 0) {
             |    $index--;
             |  } else {
             |    $index += $eval1.numElements();
             |  }
             |  $nullCheck
             |  {
             |    ${ev.value} = ${CodeGenerator.getValue(eval1, dataType, index)};
             |  }
             |}
           """.stripMargin
        })
      case _: MapType =>
        doGetValueGenCode(ctx, ev, left.dataType.asInstanceOf[MapType])
    }
  }

  override def prettyName: String = "element_at"
}
