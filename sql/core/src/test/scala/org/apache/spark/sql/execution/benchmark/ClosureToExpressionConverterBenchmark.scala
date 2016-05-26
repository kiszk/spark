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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.ClosureToExpressionConverter
import org.apache.spark.util.Benchmark

class ClosureToExpressionConverterBenchmark extends SparkFunSuite {

  type RowType = Tuple6[Short, Int, Long, Float, Double, String]

  val testSchema = ExpressionEncoder[RowType]().schema

  def doMicroBenchmark(numIters: Int): Benchmark = {
    val benchmark = new Benchmark("closure-to-exprs benchmarks", 1L, defaultNumIters = numIters)

    benchmark.addCase("boolean") { iter =>
      ClosureToExpressionConverter.convert(
        (i: RowType) => i._1 == 4,
        testSchema
      ).getOrElse(throw new RuntimeException)
    }

    benchmark.addCase("arithmetic") { iter =>
      ClosureToExpressionConverter.convert(
        (i: RowType) => i._1 + Math.sqrt(i._2 * i._2) + (i._3 / i._3) * i._2,
        testSchema
      ).getOrElse(throw new RuntimeException)
    }

    benchmark.addCase("simple branch") { iter =>
      ClosureToExpressionConverter.convert(
        (i: RowType) => i._1 + (if (i._5 > 1.0) i._2 else i._3),
        testSchema
      ).getOrElse(throw new RuntimeException)
    }

    benchmark
  }

  // Check the conversion overhead is small
  test("closure-to-expr") {
    val benchmark = doMicroBenchmark(10000)

    // Java HotSpot(TM) 64-Bit Server VM 1.8.0_31-b13 on Mac OS X 10.10.2
    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    //
    // closure-to-exprs benchmarks:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // ----------------------------------------------------------------------------------------
    // boolean                                  0 /    1          0.0      295753.0       1.0X
    // arithmetic                               1 /    2          0.0     1236260.0       0.2X
    // simple branch                            1 /    1          0.0      990534.0       0.3X
    benchmark.run()
  }
}
