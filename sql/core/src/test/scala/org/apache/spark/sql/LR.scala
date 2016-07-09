/*
 */

/**
 * Benchmark to measure performance of logistic regression
 * To run this:
 *  bin/spark-submit [options] --class org.apache.spark.sql.LR
 *    sql/core/target/spark-sql_*-tests.jar
 *    [slices] [# of points] [# of dimensions] [iterations] [master URL]
 */

package org.apache.spark.sql

import java.io.File
import java.util.Random

import scala.io.Source
import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Benchmark
import org.apache.spark.util.Utils


case class DataPointScalar(y: Double)
case class DataPointArray(x: Array[Double], y: Double)

class LR {
/*
  class LR extends SparkFunSuite with SharedSQLContext {
  test("benchmark") {
    runBenchmark(sqlContext, LR.NSLICES, LR.N, LR.D, LR.R, LR.ITERATIONS)
    LR.showSparkParamters(sqlContext.sparkContext.conf)
  }
*/

  def runBenchmark(sqlContext: SQLContext,
                   numSlices: Int, N: Int, D: Int, R: Double, ITERATIONS: Int): Unit = {
    import sqlContext.implicits._

    def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
      val (keys, values) = pairs.unzip
      val currentValues = keys.map(key => Try(sqlContext.conf.getConfString(key)).toOption)
      (keys, values).zipped.foreach(sqlContext.conf.setConfString)
      try f finally {
        keys.zip(currentValues).foreach {
          case (key, Some(value)) => sqlContext.conf.setConfString(key, value)
          case (key, None) => sqlContext.conf.unsetConf(key)
        }
      }
    }

    sqlContext.conf.setConfString(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    val rand = new Random(42)
    // initialize points data as RDD[DataPointArray]
    val pointsArray = sqlContext.sparkContext.parallelize(1 to N, numSlices).map(n => {
      val a = new Array[Double](D)
      val y = if (n % 2 == 0) -1 else 1
      var i = 0
      while(i < D) {
        a(i) = rand.nextGaussian + y * R
        i = i + 1
      }
      DataPointArray(a, n.toDouble)
    })

    // initialize weight vector
    val ns = scala.math.min(10, D)
    val w1 = new Array[Double](D)
    var i = 0
    while(i < D) {
      w1(i) = 2 * rand.nextDouble - 1
      i = i + 1
    }
    print(w1.slice(0, ns).mkString(s"initial(0-${ns-1}): [", ",", "]\n"))

    val v1 = 2 * rand.nextDouble - 1

    val w = new Array[Double](D)

    def RDDArray(pointsArray: RDD[DataPointArray], iter: Int) {
      for (i <- 0 to D-1)
        w(i) = w1(i)

      for (iter <- 1 to ITERATIONS) {
        val gradient = pointsArray
          .map(p => {
            var i = 0
            var dotp: Double = 0.0
            while (i < D) {
              dotp += w(i) * p.x(i)
              i = i + 1
            }

            val a = new Array[Double](D)
            i = 0
              while (i < D) {
              a(i) = p.x(i) * (1 / (1 + scala.math.exp(-p.y * dotp)) - 1) * p.y
              i = i + 1
            }
            a
          })
          .reduce((a, b) => {
            var i = 0
            while (i < D) {
              a(i) = a(i) + b(i)
              i = i + 1
            }
            a
          })

          i = 0
          while (i < D) {
            w(i) = w(i) - gradient(i)
            i = i + 1
          }
      } // iteration ends
      if (iter == 0) { print(w.slice(0, ns).mkString(s"final(0-${ns-1}): [", ",", "]\n")) }
    }


    def DatasetArray(pointsArray: Dataset[DataPointArray], iter: Int) {
      for (i <- 0 to D-1)
        w(i) = w1(i)

      for (iter <- 1 to ITERATIONS) {
        val gradient = pointsArray
          .map(p => {
            var i = 0
            var dotp: Double = 0.0
            while (i < D) {
              dotp += w(i) * p.x(i)
              i = i + 1
            }

            val a = new Array[Double](D)
            i = 0
            while (i < D) {
              a(i) = p.x(i) * (1 / (1 + scala.math.exp(-p.y * dotp)) - 1) * p.y
              i = i + 1
            }
            a
          })
          .reduce((a, b) => {
            var i = 0
            while (i < D) {
              a(i) = a(i) + b(i)
              i = i + 1
            }
            a
          })
          //.agg(sum("value")).head.getAs[scala.collection.mutable.WrappedArray[Double]](0)

        i = 0
        while (i < D) {
          w(i) = w(i) - gradient(i)
          i = i + 1
        }
      } // iteration ends
      if (iter == 0) { print(w.slice(0, ns).mkString(s"final(0-${ns-1}): [", ",", "]\n")) }
    }

    ///////////////////////////////
    // benchmark for array version
    ///////////////////////////////

    val benchmarkArray = new Benchmark("Array Dataset and DataFrame vs. RDD",
       5, // valuesPerIteration
       5  // minNumIters
    )

    pointsArray.cache().count()
    benchmarkArray.addCase("rddArray cache") { iter =>
      RDDArray(pointsArray, iter)
    }

    val dsArray = pointsArray.toDS()
    dsArray.cache().count()
    benchmarkArray.addCase("datasetArray cache") { iter =>
      DatasetArray(dsArray, iter)
    }

    benchmarkArray.run()

    print(s"\nBenchmark parameters: " +
      s"N=$N, D=$D, R=$R, ITERATIONS=$ITERATIONS, numSlices=$numSlices\n\n")
  }

  def run(sqlContext: SQLContext, nslices: Int, n: Int, d: Int, r: Double, iters: Int): Unit = {
    runBenchmark(sqlContext, nslices, n, d, r, iters)
  }

}

object LR {
  val NSLICES = 1
  val N = 100000 // 1024*1024*15  // Number of data points, orig 10000
  val D = 100  // Number of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 100

  def main(args: Array[String]): Unit = {
    val nslices = if (args.length > 0) args(0).toInt else NSLICES
    val n = if (args.length > 1) args(1).toInt else N
    val d = if (args.length > 2) args(2).toInt else D
    val iters = if (args.length > 3) args(3).toInt else ITERATIONS
    val masterURL = if (args.length > 4) args(4) else "local[1]"

    val conf = new SparkConf()
    val sc = new SparkContext(masterURL, "LR", conf)
    val sqlContext = new SQLContext(sc)

    val benchmark = new LR()
    benchmark.run(sqlContext, nslices, n, d, R, iters)

    showSparkParamters(sqlContext.sparkContext.conf)

    showExecutionEnvironment
  }

  def showConfigFile(filename: String): Unit = {
    try {
      val file = new File(filename)
      if (file.exists) {
        print(s"\n--- $filename ---\n")
        val lines = Source.fromFile(file).getLines
        lines.foreach(line => {
          if (line != "" && line.charAt(0) != '#') {
            print(s"  $line\n")
          }
        })
      }
    } catch {
      case e: Exception => print(s"$e\n")
    }
  }

  def showSparkParamters(sc: SparkConf): Unit = {
    print(s"\n=== Show Spark properties ===\n")
    sc.getAll.map(s => print(s"  ${s._1}: ${s._2}\n"))
  }

  def showExecutionEnvironment() : Unit = {
    print(s"\n=== Show execution environment ===\n")
    try {
      val pid = System.getProperty("sun.java.launcher.pid")
      if (pid != null) {
        val file = new File(s"/proc/$pid/cmdline")
        if (file.exists) {
          print(s"\n--- command line options ---\n")
          val lines = Source.fromFile(file).getLines
          lines.foreach(line => {
            if (line != "") {
              val l = line.replace(0.toChar, ' ')
              print(s"  $l\n")
            }
          })
        }
      }

      val sparkhome = sys.env.getOrElse("SPARK_HOME",
        {
          print("SPARK_HOME environment is not defined\n")
          return
        }
      )
      if ((new File(s"$sparkhome/conf").exists)) {
        showConfigFile(s"$sparkhome/conf/spark-defaults.conf")
        showConfigFile(s"$sparkhome/conf/spark-env.sh")
      } else {
        print(s"Cannot find $sparkhome/conf directory")
      }

      val commitID = Utils.executeAndGetOutput(
        Seq("/usr/bin/git", "log", "-n", "1", "--format=%H"),
        workingDir = new File(sparkhome))
      print(s"\n--- Commit ID for $sparkhome ---\n  $commitID\n")
      val files = Utils.executeAndGetOutput(
        Seq("/usr/bin/git", "status"), workingDir = new File(sparkhome))
      print(s"\n--- Modified files under $sparkhome ---\n$files\n")

    } catch {
      case e: Exception => print(s"$e\n")
    }
    print(s"\n=== End of show execution environment ===\n")
  }
}
