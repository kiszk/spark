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
    .getOrCreate()

  type RowType = Tuple6[Short, Int, Long, Float, Double, String]

  val testSchema = ExpressionEncoder[RowType]().schema

  def doMicroBenchmark(numIters: Int): Benchmark = {
    val benchmark = new Benchmark("closure-to-exprs microbenchmarks", 1)

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

    val benchmark = new Benchmark("end-to-end benchmark", 1)
    val ds = sparkSession.range(1, 1000000)
      .select(($"id" % 3).as("a"), ($"id" % 5).as("b"), ($"id" % 7).as("c"))
    val name = "a * b + c"
    val func = {
      val temp = ds.map(d => d.getLong(0) + d.getLong(1) + d.getLong(2))
      temp.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase(s"$name: closure.convertToExpr off", numIters) { iter =>
      sparkSession.conf.set("spark.sql.closure.convertToExpr", value = false)
      func
    }
    benchmark.addCase(s"$name: closure.convertToExpr on", numIters) { iter =>
      sparkSession.conf.set("spark.sql.closure.convertToExpr", value = true)
      func
    }
    benchmark
  }

  test("closure-to-expr") {
    val benchmark1 = doMicroBenchmark(100)
    val benchmark2 = doSimpleBenchmark(100)

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    back-to-back map:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    RDD                                      1935 / 2105         51.7          19.3       1.0X
    DataFrame                                 756 /  799        132.3           7.6       2.6X
    Dataset                                  7359 / 7506         13.6          73.6       0.3X
    */
    benchmark1.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    back-to-back map:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    RDD                                      1935 / 2105         51.7          19.3       1.0X
    DataFrame                                 756 /  799        132.3           7.6       2.6X
    Dataset                                  7359 / 7506         13.6          73.6       0.3X
    */
    benchmark2.run()
  }
}
