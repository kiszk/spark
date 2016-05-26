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

// scalastyle:off
// TODO: fix

package org.apache.spark.sql.catalyst.expressions

import javassist.ClassClassPath

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{ExprCode, CodegenContext}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

import scala.collection.mutable

import javassist.{Modifier, ClassPool, CtMethod}
import javassist.bytecode.Opcode._
import javassist.bytecode.{ConstPool, InstructionPrinter}
import javassist.bytecode.analysis.Analyzer

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

object ClosureToExpressionConverter extends Logging {

  val analyzer = new Analyzer()
  val classPool = ClassPool.getDefault

  def analyzeMethod(
      method: CtMethod,
      schema: StructType,
      localVars: Array[Expression],
      stack: mutable.Stack[Expression] = mutable.Stack.empty[Expression],
      pos: Int = 0,
      level: Int = 0): Option[Expression] = {
    logDebug("-" * 80)
    logDebug(s"level: ${level}")
    logDebug(s"method: ${method.getLongName}")
    logDebug(s"localVars: ${localVars.toSeq}")
    logDebug("-" * 80)

    val instructions = method.getMethodInfo().getCodeAttribute.iterator()
    // Move the point of current instruction to given position (default is zero).
    instructions.move(pos)
    val constPool = method.getMethodInfo.getConstPool

    if (log.isDebugEnabled) {
      val codes = method.getMethodInfo().getCodeAttribute.iterator()
      while (codes.hasNext) {
        val pos = codes.next()
        val code = InstructionPrinter.instructionString(codes, pos, constPool)
        logDebug(s"${pos}: ${code}")
      }
    }

    def ldc(pos: Int): Any = _ldc(pos, instructions.byteAt(pos + 1))
    def ldcw(pos: Int): Any = _ldc(pos, instructions.u16bitAt(pos + 1))
    def _ldc(pos: Int, cp_index: Int): Any = {
      constPool.getTag(cp_index) match {
        case ConstPool.CONST_Double => constPool.getDoubleInfo(cp_index)
        case ConstPool.CONST_Float => constPool.getFloatInfo(cp_index)
        case ConstPool.CONST_Integer => constPool.getIntegerInfo(cp_index)
        case ConstPool.CONST_Long => constPool.getLongInfo(cp_index)
        case ConstPool.CONST_String => constPool.getStringInfo(cp_index)
      }
    }

    def getInvokeMethodTarget(pos: Int) = {
      val mrClassName = constPool.getMethodrefClassName(instructions.u16bitAt(pos + 1))
      val mrMethName = constPool.getMethodrefName(instructions.u16bitAt(pos + 1))
      val mrDesc = constPool.getMethodrefType(instructions.u16bitAt(pos + 1))
      val ctClass = classPool.get(mrClassName)
      ctClass.getMethod(mrMethName, mrDesc)
    }

    def getInvokeInterfaceTarget(pos: Int) = {
      val mrClassName = constPool.getInterfaceMethodrefClassName(instructions.u16bitAt(pos + 1))
      val mrMethName = constPool.getInterfaceMethodrefName(instructions.u16bitAt(pos + 1))
      val mrDesc = constPool.getInterfaceMethodrefType(instructions.u16bitAt(pos + 1))
      val ctClass = classPool.get(mrClassName)
      ctClass.getMethod(mrMethName, mrDesc)
    }

    while (instructions.hasNext) {
      // Fetch next op code
      val pos = instructions.next()
      val op = instructions.byteAt(pos)
      logDebug("*" * 20 + " " + InstructionPrinter.instructionString(instructions, pos, constPool)
        + s" (pos=$pos, level=$level stack=${stack.size}}) " + "*" * 20)

      // TODO: check supports, ops and #parameters

      def analyzeCMP(
          value1: Expression,
          value2: Expression,
          compOp: (Expression, Expression) => Predicate): Option[Expression] = {
        val trueJumpTarget = instructions.s16bitAt(pos + 1) + pos
        val trueExpression = analyzeMethod(
          method,
          schema,
          localVars,
          stack,
          trueJumpTarget,
          level)
        val falseExpression = analyzeMethod(
          method,
          schema,
          localVars,
          stack,
          instructions.next(),
          level)
        if (trueExpression.isDefined && falseExpression.isDefined) {
          Some(If(compOp(value1, value2), trueExpression.get, falseExpression.get))
        } else {
          None
        }
      }

      def analyzeIFNull(
          compOp: (Expression) => Predicate): Option[Expression] = {
        val cmp = stack.pop
        val trueJumpTarget = instructions.s16bitAt(pos + 1) + pos
        val trueExpression = analyzeMethod(
          method,
          schema,
          localVars,
          stack,
          trueJumpTarget,
          level)
        val falseExpression = analyzeMethod(
          method,
          schema,
          localVars,
          stack,
          instructions.next(),
          level)
        if (trueExpression.isDefined && falseExpression.isDefined) {
          Some(If(compOp(cmp), trueExpression.get, falseExpression.get))
        } else {
          None
        }
      }

      op match {
        // load a reference onto the stack from local variable {0, 1, 2}
        case ALOAD_0 => stack.push(localVars(0))
        case ALOAD_1 => stack.push(localVars(1))
        case ALOAD_2 => stack.push(localVars(2))
        case ALOAD_3 => stack.push(localVars(3))

        case ASTORE_0 => localVars(0) = stack.pop
        case ASTORE_1 => localVars(1) = stack.pop
        case ASTORE_2 => localVars(2) = stack.pop
        case ASTORE_3 => localVars(3) = stack.pop

        // load an int value from local variable {0, 1, 2}
        case ILOAD_0 => stack.push(localVars(0))
        case ILOAD_1 => stack.push(localVars(1))
        case ILOAD_2 => stack.push(localVars(2))
        case ILOAD_3 => stack.push(localVars(3))

        case LLOAD_0 => stack.push(localVars(0))
        case LLOAD_1 => stack.push(localVars(1))
        case LLOAD_2 => stack.push(localVars(2))
        case LLOAD_3 => stack.push(localVars(2))

        case FLOAD_0 => stack.push(localVars(0))
        case FLOAD_1 => stack.push(localVars(1))
        case FLOAD_2 => stack.push(localVars(2))
        case FLOAD_3 => stack.push(localVars(3))

        case DLOAD_0 => stack.push(localVars(0))
        case DLOAD_1 => stack.push(localVars(1))
        case DLOAD_2 => stack.push(localVars(2))
        case DLOAD_3 => stack.push(localVars(3))

        // push a byte onto the stack as an integer value
        case BIPUSH =>
          // TODO: byte must be sign-extended into an integer value?
          stack.push(Literal(instructions.byteAt(pos + 1)))

        // load the int value {0, 1, ..} onto the stack
        case ICONST_0 => stack.push(Literal(0))
        case ICONST_1 => stack.push(Literal(1))
        case ICONST_2 => stack.push(Literal(2))
        case ICONST_3 => stack.push(Literal(3))
        case ICONST_4 => stack.push(Literal(4))
        case ICONST_5 => stack.push(Literal(5))

        // push the long {0, 1} onto the stack
        case LCONST_0 => stack.push(Literal(0L))
        case LCONST_1 => stack.push(Literal(1L))

        case FCONST_0 => stack.push(Literal(0.0f))
        case FCONST_1 => stack.push(Literal(1.0f))
        case FCONST_2 => stack.push(Literal(2.0f))

        case DCONST_0 => stack.push(Literal(0.0d))
        case DCONST_1 => stack.push(Literal(1.0d))

        // goes to another instruction at branchoffset: byte at [pos + 1] << 8 + byte at [pos + 2].
        // we directly fetch 2 bytes here.
        case GOTO =>
          val target = instructions.s16bitAt(pos + 1) + pos
          return analyzeMethod(method, schema, localVars, stack, target, level)

        // convert an int into a byte
        case I2B => stack.push(Cast(stack.pop, ByteType))
        // convert an int into a character
        case I2C => stack.push(Cast(stack.pop, StringType))
        // convert an int into a double
        case I2D => stack.push(Cast(stack.pop, DoubleType))
        // convert an int into a float
        case I2F => stack.push(Cast(stack.pop, FloatType))
        // convert an int into a long
        case I2L => stack.push(Cast(stack.pop, LongType))
        // convert an int into a short
        case I2S => stack.push(Cast(stack.pop, ShortType))

        case L2D => stack.push(Cast(stack.pop, DoubleType))
        case L2F => stack.push(Cast(stack.pop, FloatType))
        case L2I => stack.push(Cast(stack.pop, IntegerType))

        case F2D => stack.push(Cast(stack.pop, DoubleType))
        case F2I => stack.push(Cast(stack.pop, IntegerType))
        case F2L => stack.push(Cast(stack.pop, LongType))

        // multiply two integers/longs/floats/doubles
        case IMUL | LMUL | FMUL | DMUL =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Multiply(stackHeadMinus1, stackHead))

        // divide two integers/longs/floats/doubles
        case IDIV| LDIV| FDIV| DDIV =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Divide(stackHeadMinus1, stackHead))

        // add two integers/longs/floats/doubles
        case IADD | LADD | FADD | DADD =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Add(stackHeadMinus1, stackHead))

        // subtract two integers/longs/floats/doubles
        case ISUB| LSUB | FSUB| DSUB =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Subtract(stackHeadMinus1, stackHead))

        // push a constant at #index = [pos + 1] from a constant pool
        // (string, int or float) into stack
        case LDC => stack.push(Literal(ldc(pos)))
        // push a constant at #index = [pos + 1] << 8 + [pos + 2] from a constant pool
        // (string, int or float) onto the stack

        case LDC_W => stack.push(Literal(ldcw(pos)))

        // push a constant at #index = [pos + 1] << 8 + [pos + 2] from a constant pool
        // (double or long) onto the stack
        // In JVM, this pushes two words onto the stack, but we're going to only push one.
        case LDC2_W => stack.push(Literal(ldcw(pos)))

        case IF_ICMPGE =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          return analyzeCMP(stackHeadMinus1, stackHead, GreaterThanOrEqual)

        case IF_ICMPGT =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          return analyzeCMP(stackHeadMinus1, stackHead, GreaterThan)

        // if stack.popMinus1 is less than or equal to stack.pop,
        // branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        case IF_ICMPLE =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          return analyzeCMP(stackHeadMinus1, stackHead, LessThanOrEqual)

        case IF_ICMPLT =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          return analyzeCMP(stackHeadMinus1, stackHead, LessThan)

        // if stack.popMinus1 and stack.pop are not equal,
        // branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        case IF_ICMPNE =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          return analyzeCMP(stackHeadMinus1, stackHead, (e1, e2) => Not(EqualTo(e1, e2)))

        case LCMP =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Subtract(stackHeadMinus1, stackHead))

        case FCMPG =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Subtract(stackHead, stackHeadMinus1))

        case FCMPL =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Subtract(stackHeadMinus1, stackHead))

        case DCMPG =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Subtract(stackHead, stackHeadMinus1))

        case DCMPL =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          stack.push(Subtract(stackHeadMinus1, stackHead))

        // if stack.pop is not 0, branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        // the foillowing branching ops follow the same sematics.
        case IFNE => return analyzeCMP(stack.pop, Literal(0), (e1, e2) => Not(EqualTo(e1, e2)))
        case IFGT => return analyzeCMP(stack.pop, Literal(0), (e1, e2) => GreaterThan(e1, e2))
        case IFGE =>
          return analyzeCMP(stack.pop, Literal(0), (e1, e2) => GreaterThanOrEqual(e1, e2))
        case IFLT => return analyzeCMP(stack.pop, Literal(0), (e1, e2) => LessThan(e1, e2))
        case IFLE => return analyzeCMP(stack.pop, Literal(0), (e1, e2) => LessThanOrEqual(e1, e2))
        case IFEQ => return analyzeCMP(stack.pop, Literal(0), (e1, e2) => EqualTo(e1, e2))
        case IFNONNULL => return analyzeIFNull((e) => IsNotNull(e))
        case IFNULL => return analyzeIFNull((e) => IsNull(e))

        case CHECKCAST =>
          // TODO: Need this type-check for input data?
          // It seems the type-check has already been done inside Dataset.

          // val cp_index = instructions.u16bitAt(pos + 1)
          // val className = constPool.getClassInfo(cp_index)
          // stack.push(CheckCast(stack.pop, Literal(className)))

        case ARRAYLENGTH => stack.push(Length(stack.pop))

        case INVOKEINTERFACE =>
          val target = getInvokeInterfaceTarget(pos)
          if (target.getDeclaringClass.getName == classOf[Row].getName) {
            if (target.getName.startsWith("get")) {
              val (stackHead, _) = (stack.pop, stack.pop)
              val fieldNumber = stackHead.asInstanceOf[Literal].value.asInstanceOf[Int]
              stack.push(NPEOnNull(UnresolvedAttribute(schema.fields(fieldNumber).name)))
            } else if (target.getName == "isNullAt") {
              val stackHead = stack.pop
              val fieldNumber = stackHead.asInstanceOf[Literal].value.asInstanceOf[Int]
              stack.push(IsNull(UnresolvedAttribute(schema.fields(fieldNumber).name)))
            } else {
              logInfo("TODO: error message")
              return None
            }
          } else {
            // TODO: error message
            logInfo("TODO: error message")
            return None
          }

        case INVOKESTATIC =>
          val target = getInvokeMethodTarget(pos)
          // TODO: Map some math functions into expressions.mathExpressions.*
          if (target.getName == "sqrt") {
            stack.push(Sqrt(stack.pop))
          } else if (target.getName == "log10") {
            stack.push(Log10(stack.pop))
          } else {
            val localVars = new Array[Expression](4)
            val numParameters = target.getParameterTypes.length
            for (i <- 0 until numParameters) {
              localVars(i) = stack(i)
            }
            analyzeMethod(target, schema, localVars, level = level + 1) match {
              case Some(expr) =>
                // Drop used entries in the stack
                (0 until numParameters).map(_ => stack.pop)
                stack.push(expr)
              case None =>
                logInfo("problem analyzing static method call")
                return None
            }
          }

        case INVOKEVIRTUAL =>
          val target = getInvokeMethodTarget(pos)
          val numParameters = target.getParameterTypes.length
          target.getDeclaringClass.getName match {
            case cls: String if cls.startsWith("scala.Tuple") =>
              // Drop used entries in the stack
              (0 to numParameters).map(_ => stack.pop)
              target.getName match {
                case field: String if field.matches("^_[12]?[0-9].*$") =>
                  // Remove an unnecessary suffix in case of `Tuple2`
                  val name = field.replace("$mcI$sp", "")
                  stack.push(UnresolvedAttribute(name))
                case _ =>
                  logInfo(s"unknown target ${target.getName}")
                  return None
              }
            case _ =>
              val localVars = new Array[Expression](4)
              localVars(0) = stack.top // Set objectref
              for (i <- 0 until numParameters) {
                localVars(i + 1) = stack(i)
              }
              analyzeMethod(target, schema, localVars, level = level + 1) match {
                case Some(expr) =>
                  // Drop used entries in the stack
                  (0 to numParameters).map(_ => stack.pop)
                  stack.push(expr)
                case None =>
                  logInfo("problem analyzing method call")
                  return None
              }
          }

        case GETFIELD =>
          val target = classPool.get(constPool.getFieldrefClassName(instructions.u16bitAt(pos + 1)))
          val targetField = constPool.getFieldrefName(instructions.u16bitAt(pos + 1))
          target.getName match {
            case cls: String if cls.startsWith("scala.Tuple") =>
              stack.push(UnresolvedAttribute(targetField))
            case cls: String if Seq("java.lang.String", "java.lang.Short", "java.lang.Integer",
                "java.lang.Long", "java.lang.Float", "java.lang.Double").contains(cls) =>
              // Do nothing here
            case _ =>
              logInfo(s"unknown GETFIELD target: ${target.getName}")
              return None
          }

        case GETSTATIC =>
          val target = classPool.get(constPool.getFieldrefClassName(instructions.u16bitAt(pos + 1)))
          val targetField = constPool.getFieldrefName(instructions.u16bitAt(pos + 1))
          if (target.getName == "java.lang.Boolean") {
            stack.push(Literal(java.lang.Boolean.valueOf(targetField)))
          } else {
            logInfo(s"unknown GETSTATIC target: ${target.getName}")
            return None
          }

        case DRETURN | FRETURN | IRETURN | LRETURN | ARETURN =>
          return Some(stack.top)

        case _ =>
          logInfo(s"unknown opcode $op");
          return None
      }

      if (log.isDebugEnabled()) {
        stack.zipWithIndex.foreach { case (expr, idx) =>
          logDebug(s"  $idx=$expr")
        }
      }
    }

    logInfo("oh no!")
    return None
  }

  def isStatic(method: CtMethod): Boolean = Modifier.isStatic(method.getModifiers)

  // We assume the max number of local vars is 4
  private def toLocalVars(exprs: Expression*) = {
    assert(exprs.size <= 4)
    val vars = new Array[Expression](4)
    exprs.zipWithIndex.map { case (expr, i) =>
        vars(i) = exprs(i)
    }
    vars
  }

  // TODO: handle argument types
  // For now, this assumes f: Row => Expression
  def convert(closure: Object, schema: StructType): Option[Expression] = try {
    val clazz = closure.getClass
    classPool.insertClassPath(new ClassClassPath(clazz))
    val ctClass = classPool.get(clazz.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    // Take the apply() that has the minimum number of converted expressions
    applyMethods.flatMap { method =>
      logDebug(" \n  " * 10)
      assert(method.getParameterTypes.length == 1)
      val expr = if (isStatic(method)) {
        analyzeMethod(method, schema, toLocalVars(UnresolvedAttribute("value")))
      } else {
        analyzeMethod(method, schema, toLocalVars(UnresolvedAttribute("this"),
          UnresolvedAttribute("value")))
      }
      expr.map { e =>
        val numExpr = e.collect { case e => true }.size
        logInfo(s"numExpr:${numExpr} expr:${e}")
        (numExpr, e)
      }
    }.sortBy(_._1).map(_._2).headOption
  } catch {
    // Fall back to a regular path
    case e: Exception =>
      logInfo(s"failed to convert into exprs because ${e.getMessage}")
      None
  }

  def convertMap(closure: Object, schema: StructType): Option[Expression] = {
    convert(closure, schema)
  }

  def convertFilter(closure: Object, schema: StructType): Option[Expression] = {
    // TODO: If optimized plans have any casts in filters, it seems codegen does not work well.
    // Later, we'll need to look into this issue.
    convert(closure, schema).map { expr => Cast(expr, BooleanType) }
  }

}

/**
 * An expression that tests if the given expression can be casted to a type.
 */
case class CheckCast(left: Expression, right: Expression)
    extends BinaryExpression with ExpectsInputTypes with NonSQLExpression {
  override def nullable: Boolean = false

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(left.dataType, StringType)

  override def eval(input: InternalRow): Any = {
    val result = left.eval(input)
    val castToType = Utils.classForName(right.eval(input).asInstanceOf[UTF8String].toString)
    if (result.getClass.isAssignableFrom(castToType)) {
      result
    } else {
      throw new ClassCastException
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = left.genCode(ctx)
    val castToTypeName = right.genCode(ctx)
    val castCode = s"""
      ${eval.code}
      ${castToTypeName.code}
      (${castToTypeName.value})(${eval.value});
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${eval.value};
      """
    ev.copy(code = eval.code + castCode)
  }
}

/**
 * An expression that throws NullPointerException on null input values.
 */
case class NPEOnNull(child: Expression) extends UnaryExpression with NonSQLExpression {
  override def nullable: Boolean = false

  override def dataType: DataType = child.dataType

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) throw new NullPointerException
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val npeCode = s"""
      ${eval.code}
      if(${eval.isNull}) { throw new NullPointerException(); }
      ${ctx.javaType(dataType)} ${ev.value} = ${eval.value};
      """
    ev.copy(code = eval.code + npeCode)
  }
}
