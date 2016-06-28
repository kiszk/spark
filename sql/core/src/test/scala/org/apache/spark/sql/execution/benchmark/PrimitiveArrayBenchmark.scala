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

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.util.Benchmark

/**
 * Benchmark [[PrimitiveArray]] for DataFrame and Dataset program using primitive array
 * To run this:
 *  build/sbt "sql/test-only *benchmark.GenericArrayDataBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class PrimitiveArrayBenchmark extends BenchmarkBase {
  import sparkSession.implicits._

  new SparkConf()
    .setMaster("local[1]")
    .setAppName("microbenchmark")
    .set("spark.driver.memory", "3g")

  def showArray(iters: Int): Unit = {
    val count = 1024 * 1024 * 24

    val sc = sparkSession.sparkContext
    val primitiveIntArray = Array.fill[Int](count)(1)
    val dfInt = sc.parallelize(Seq(primitiveIntArray), 1).toDF
    dfInt.count
    val intArray = { i: Int =>
      var n = 0
      while (n < iters) {
        dfInt.selectExpr("value[0]").count
        n += 1
      }
    }
    val primitiveDoubleArray = Array.fill[Double](count)(1.0)
    val dfDouble = sc.parallelize(Seq(primitiveDoubleArray), 1).toDF
    dfDouble.count
    val doubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        dfDouble.selectExpr("value[0]").count
        n += 1
      }
    }

    val benchmark = new Benchmark("Read an array in DataFrame", count * iters)
    benchmark.addCase("Int   ")(intArray)
    benchmark.addCase("Double")(doubleArray)
    benchmark.run
    /*
    Without SPARK-16043
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read an array in DataFrame:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                           1078 / 1205         23.4          42.8       1.0X
    Double                                        4844 / 5241          5.2         192.5       0.2X
    */
    /*
    With SPARK-16043
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read an array in DataFrame:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                            502 /  530         50.1          20.0       1.0X
    Double                                        1111 / 1170         22.7          44.1       0.5X
    */
  }

  ignore("Show an array in DataFrame") {
    showArray(1)
  }

  def main(args: Array[String]): Unit = {
    showArray(1)
  }
}
