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
      children: Seq[Expression],
      stack: mutable.Stack[Expression] = mutable.Stack.empty[Expression],
      pos: Int = 0,
      level: Int = 0): Option[Expression] = {
    logDebug("-" * 80)
    logDebug(s"level: ${level}")
    logDebug(s"method: ${method.getLongName}")
    logDebug(s"children: ${children}")
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

      // How the stack will grow after this op code:
      // For example, I2L (convert an int into a long) will pop a value from stack and push
      // its result into it. So the grown number is 0.
      val stackGrow: Int = {
        op match {
          // case INVOKEVIRTUAL => 1 - getInvokeMethodTarget(pos).getParameterTypes.length
          // case INVOKESTATIC => 1 - getInvokeMethodTarget(pos).getParameterTypes.length
          // case INVOKEINTERFACE => 1 - getInvokeInterfaceTarget(pos).getParameterTypes.length
          case LDC2_W => 1 // TODO: in reality, this pushes 2; this is a hack.
          case GETFIELD => 1 // hack
          case LCONST_0 | LCONST_1 => 1 // hack
          case LADD | DADD | LMUL | DMUL => -1
          case I2B | I2C | I2F | I2S | I2L | I2D => 0 // hack
          case _ => STACK_GROW(op) // pre-defined stack grow in javassist
        }
      }

      def analyzeCMP(
          value1: Expression,
          value2: Expression,
          compOp: (Expression, Expression) => Predicate): Option[Expression] = {
        val trueJumpTarget = instructions.s16bitAt(pos + 1) + pos
        val trueExpression = analyzeMethod(
          method,
          schema,
          children,
          stack,
          trueJumpTarget,
          level)
        val falseExpression = analyzeMethod(
          method,
          schema,
          children,
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
        val trueJumpTarget = instructions.s16bitAt(pos + 1) + pos
        val trueExpression = analyzeMethod(
          method,
          schema,
          children,
          stack,
          trueJumpTarget,
          level)
        val falseExpression = analyzeMethod(
          method,
          schema,
          children,
          stack,
          instructions.next(),
          level)
        if (trueExpression.isDefined && falseExpression.isDefined) {
          Some(If(compOp(stack.pop), trueExpression.get, falseExpression.get))
        } else {
          None
        }
      }

      op match {
        // load a reference onto the stack from local variable {0, 1, 2}
        case ALOAD_0 => stack.push(children(0))
        case ALOAD_1 => stack.push(children(1))
        case ALOAD_2 => stack.push(children(2))

        // push a byte onto the stack as an integer value
        case BIPUSH => stack.push(Literal(instructions.byteAt(pos + 1))) // TODO: byte must be sign-extended into an integer value?

        // load the int value {0, 1, ..} onto the stack
        case ICONST_0 => stack.push(Literal(0))
        case ICONST_1 => stack.push(Literal(1))
        case ICONST_2 => stack.push(Literal(2))
        case ICONST_3 => stack.push(Literal(3))
        case ICONST_4 => stack.push(Literal(4))

        // push the long {0, 1} onto the stack
        case LCONST_0 => stack.push(Literal(0L))
        case LCONST_1 => stack.push(Literal(1L))

        // load an int value from local variable {0, 1, 2}
        case ILOAD_0 => stack.push(children(0))
        case ILOAD_1 => stack.push(children(1))
        case ILOAD_2 => stack.push(children(2))

        // goes to another instruction at branchoffset: byte at [pos + 1] << 8 + byte at [pos + 2].
        // we directly fetch 2 bytes here.
        case GOTO =>
          val target = instructions.s16bitAt(pos + 1) + pos
          return analyzeMethod(method, schema, children, stack, target, level)

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

        // if stack.popMinus1 is less than or equal to stack.pop,
        // branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        case IF_ICMPLE =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          return analyzeCMP(stackHeadMinus1, stackHead, LessThanOrEqual)

        // if stack.popMinus1 and stack.pop are not equal,
        // branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        case IF_ICMPNE =>
          val (stackHead, stackHeadMinus1) = (stack.pop, stack.pop)
          return analyzeCMP(stackHeadMinus1, stackHead, (e1, e2) => Not(EqualTo(e1, e2)))

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
          val cp_index = instructions.u16bitAt(pos + 1)
          val className = constPool.getClassInfo(cp_index)
          stack.push(CheckCast(stack.pop, Literal(className)))

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
              throw new Exception("TODO: error message")
            }
          } else {
            // TODO: error message
            throw new Exception("TODO: error message")
          }

        case INVOKESTATIC =>
          val target = getInvokeMethodTarget(pos)
          val numParameters = target.getParameterTypes.length
          val attributes = (0 until numParameters).map(i => stack.pop)
          analyzeMethod(target, schema, attributes, level = level + 1) match {
            case Some(expr) => stack.push(expr)
            case None =>
              throw new Exception("problem analyzing static method call")
          }

        case INVOKEVIRTUAL =>
          val target = getInvokeMethodTarget(pos)
          val numParameters = target.getParameterTypes.length
          val attributes = (0 to numParameters).map(i => stack.pop)
          target.getDeclaringClass.getName match {
            case cls: String if cls.startsWith("scala.Tuple") =>
              target.getName match {
                case field: String if field.matches("^_[12]?[0-9]$") =>
                  stack.push(UnresolvedAttribute(field))
                // `Tuple2` matches this case
                case field: String if field.matches("^_[12]?[0-9]\\$mcI\\$sp$") =>
                  stack.push(UnresolvedAttribute(field.replace("$mcI$sp", "")))
                case _ =>
                  throw new Exception(s"unknown target ${target.getName}")
              }
            case _ =>
              analyzeMethod(target, schema, attributes, level = level + 1) match {
                case Some(expr) => stack.push(expr)
                case None =>
                  throw new Exception("problem analyzing method call")
              }
          }

        case GETFIELD =>
          val target = classPool.get(constPool.getFieldrefClassName(instructions.u16bitAt(pos + 1)))
          val targetField = constPool.getFieldrefName(instructions.u16bitAt(pos + 1))
          target.getName match {
            case cls: String if cls.startsWith("scala.Tuple") =>
              stack.push(UnresolvedAttribute(targetField))
              // TODO: Since `stackGrow` has 1 here, this op generates duplicated entries in the stack
            case cls: String if Seq("java.lang.Integer", "java.lang.Double").contains(cls) =>
              // Do nothing
            case _ =>
              throw new Exception(s"unknown GETFIELD target: ${target.getName}")
          }

        case GETSTATIC =>
          val target = classPool.get(constPool.getFieldrefClassName(instructions.u16bitAt(pos + 1)))
          val targetField = constPool.getFieldrefName(instructions.u16bitAt(pos + 1))
          if (target.getName == "java.lang.Boolean") {
            stack.push(Literal(java.lang.Boolean.valueOf(targetField)))
          } else {
            throw new Exception(s"unknown GETSTATIC target: ${target.getName}")
          }

        case DRETURN | IRETURN | LRETURN | ARETURN =>
          return Some(stack.pop)

        case _ =>
          throw new Exception(s"unknown opcode $op");
      }

      if (log.isDebugEnabled()) {
        stack.zipWithIndex.foreach { case (expr, idx) =>
          logDebug(s"  $idx=$expr")
        }
      }
    }

    throw new Exception("oh no!")
  }

  def isStatic(method: CtMethod): Boolean = Modifier.isStatic(method.getModifiers)

  // TODO: handle argument types
  // For now, this assumes f: Row => Expression
  def convert(closure: Object, schema: StructType): Option[Expression] = try {
    val clazz = closure.getClass
    classPool.insertClassPath(new ClassClassPath(clazz))
    val ctClass = classPool.get(clazz.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    // Take the first apply() method which can be resolved to an expression
    applyMethods.headOption.flatMap { method =>
      // logDebug(" \n  " * 10)
      assert(method.getParameterTypes.length == 1)
      // This expr is not actually used for a final output
      val attributes = Seq(UnresolvedAttribute("input"))
      if (isStatic(method)) {
        analyzeMethod(method, schema, attributes)
      } else {
        analyzeMethod(method, schema, Seq(UnresolvedAttribute("this")) ++ attributes)
      }
    }
  } catch {
    // Fall back to a regular path
    case e: Exception =>
      logInfo(s"failed to convert into exprs because ${e.getMessage}")
      None
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
      new ClassCastException
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
