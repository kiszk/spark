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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.ClosureToExpressionConverter
import org.apache.spark.util.Benchmark

class ClosureToExpressionConverterBenchmark extends SparkFunSuite {

  lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("benchmark")
    .config("spark.sql.codegen.wholeStage", true)
    .config("spark.sql.closure.convertToExpr", true)
    .getOrCreate()

  type RowType = Tuple6[Short, Int, Long, Float, Double, String]

  val testSchema = ExpressionEncoder[RowType]().schema

  def doMicroBenchmark(numIters: Int): Benchmark = {
    val benchmark = new Benchmark("closure-to-exprs microbenchmarks", numIters)

    benchmark.addCase("boolean") { iter =>
      for (i <- 0 until numIters) {
        ClosureToExpressionConverter.convert(
          (i: RowType) => i._1 == 4,
          testSchema
        ).get
      }
    }

    benchmark.addCase("arithmetic") { iter =>
      for (i <- 0 until numIters) {
        ClosureToExpressionConverter.convert(
          (i: RowType) => i._1 + i._2 * i._2 + (i._3 / i._3) * i._2,
          testSchema
        ).get
      }
    }

    benchmark.addCase("simple branch") { iter =>
      for (i <- 0 until numIters) {
        ClosureToExpressionConverter.convert(
          (i: RowType) => i._1 + (if (i._5 > 1.0) i._2 else i._3),
          testSchema
        ).get
      }
    }

    benchmark
  }

  def doSimpleBenchmark(numIters: Int): Benchmark = {
    import sparkSession.implicits._

    val benchmark = new Benchmark("end-to-end benchmark", numIters)
    val df = sparkSession.range(1, 10000000000L)
      .select(($"id" % 3).as("a"), ($"id" % 5).as("b"), ($"id" % 7).as("c"))
    val name = "a * b + c"
    val func = {
      val temp = df.map(d => d.getLong(0) * d.getLong(1) + d.getLong(2))
      temp.explain
      temp.queryExecution.toRdd.foreach(_ => Unit)
    }
    benchmark.addCase(s"$name: closure.convertToExpr off", numIters) { iter =>
      sparkSession.conf.set("spark.sql.closure.convertToExpr", value = true)
      func
    }
    benchmark.addCase(s"$name: closure.convertToExpr on", numIters) { iter =>
      sparkSession.conf.set("spark.sql.closure.convertToExpr", value = true)
      func
    }
    benchmark
  }

  test("closure-to-expr") {
    val benchmark1 = doMicroBenchmark(100000)
    val benchmark2 = doSimpleBenchmark(100)

    // TODO: Write benchmark results
    benchmark1.run()

    // TODO: Write benchmark results
    benchmark2.run()
  }
}
