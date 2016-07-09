package org.apache.spark.sql

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.sql.{Date, Timestamp}
import java.util.Random

import scala.util.Try

import org.apache.spark.SparkEnv
import scala.math._

import org.apache.spark.sql.functions._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.util.Benchmark

import org.apache.spark.sql.internal.SQLConf

case class DataPointScalar(y: Double)
case class DataPointArray(x: Array[Double], y: Double)

object LR {
   val N = 100000 // 1024*1024*15  // Number of data points, orig 10000
   val D = 100   // Number of dimensions
   val R = 0.7  // Scaling factor
   val ITERATIONS = 100
   val rand = new Random(42)


   def main(args: Array[String]): Unit = {

      val sparkConf = new SparkConf().setAppName("LR")
      val sc = new SparkContext(sparkConf)

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
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

      val numSlices = if (args.length > 0) args(0).toInt else 2

      // initialize points data as RDD[DataPointArray]
      val pointsArray = sc.parallelize(1 to N, 1).map(n => {
         val a = new Array[Double](D)
         val y = if (n % 2 == 0) -1 else 1
         var i = 0
         while(i < D) {
           a(i) = rand.nextGaussian + y * R
           i = i + 1
         }
         DataPointArray(a, n.toDouble)
      })

      val pointsScalar = sc.parallelize(1 to N, 1).map(n => {
         DataPointScalar(n.toDouble)
      })


      // initialize weight vector
      val w1 = new Array[Double](D)
      var i = 0
      while(i < D) {
         w1(i) = 2 * rand.nextDouble - 1
         i = i + 1
      }

      println(w1.mkString("initial: [", ",", "]"))

      val v1 = 2 * rand.nextDouble - 1

      val w = new Array[Double](D)

      def RDDArray(pointsArray: RDD[DataPointArray]) {
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
//         println(w.mkString("final: [", ",", "]"))
      }


      def DatasetArray(pointsArray: Dataset[DataPointArray]) {
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
/*
               .reduce((a, b) => {
                  var i = 0
                  while (i < D) {
                     a(i) = a(i) + b(i)
                     i = i + 1
                  }
                  a
               })
*/
            .agg(sum("value")).head.getAs[scala.collection.mutable.WrappedArray[Double]](0)

            i = 0
            while (i < D) {
               w(i) = w(i) - gradient(i)
               i = i + 1
            }
         } // iteration ends
//         println(w.mkString("final: [", ",", "]"))
      }

      /////////////////////////////
      // scalar version
      /////////////////////////////

      def RDDScalar(points: RDD[DataPointScalar]) {
         var v = v1 
         for (iter <- 1 to ITERATIONS) {
            val gradient = points
            .map(p => {
               scala.math.exp(p.y)
               })
            .reduce((a, b) => {
               a+b
            })
            v = v - gradient
         }
         println(s"final: $v")
      }

      def DatasetScalar(points: Dataset[DataPointScalar]) {
         var v = v1 
         for (iter <- 1 to ITERATIONS) {
            //println("On iteration " + iter)
            val gradient = points
            .map(p => {
               scala.math.exp(p.y)
               })
            .agg(sum("value")).first().getDouble(0)
            v = v - gradient
         }
         println(s"final: $v")
      }


      def DataFrameScalar(points: DataFrame) {
         var v = v1 
         for (iter <- 1 to ITERATIONS) {
            //println("On iteration " + iter)
            val gradient = points
            .agg(sum( points("y")*points("y")*points("y"))).first().getDouble(0)
            v = v - gradient
         }
         println(s"final: $v")
      }

      /////////////////////////////////
      // benchmark for  scalar version
      /////////////////////////////////

      /*
      val benchmarkScalar = new Benchmark("Scalar Dataset and DataFrame vs. RDD", 5, 3)

      pointsScalar.cache().count()

      benchmarkScalar.addCase("rddScalar cache") { iter =>
         RDDScalar(pointsScalar)
      }

      val ds = pointsScalar.toDS()
      ds.cache().count()

      benchmarkScalar.addCase("datasetScalar") { iter =>
         withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> "false") { 
              DatasetScalar(ds)
         }
      }


      benchmarkScalar.addCase("datasetScalar column_vector_codegen") { iter =>
         withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> "true") { 
              DatasetScalar(ds)
         }
      }

      val df = pointsScalar.toDF()
      df.cache().count()
      benchmarkScalar.addCase("dataframeScalar") { iter =>
         withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> "false") { 
            DataFrameScalar(df)
         }
      }
      benchmarkScalar.addCase("dataframeScalar column_vector_codegen") { iter =>
         withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> "true") { 
            DataFrameScalar(df)
         }
      }

      benchmarkScalar.run()
      */

      ///////////////////////////////
      // benchmark for array version
      ///////////////////////////////

      val benchmarkArray = new Benchmark("Array Dataset and DataFrame vs. RDD", 5, 5)

      pointsArray.cache().count() 
      benchmarkArray.addCase("rddArray cache") { iter =>
         RDDArray(pointsArray)
      }


      val dsArray = pointsArray.toDS()
      dsArray.cache().show()  // probably not needed since RDD was cached already

      benchmarkArray.addCase("datasetArray cache") { iter =>
         //withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> "false") {  
              DatasetArray(dsArray)
         //}
      }

      /*
      benchmarkArray.addCase("datasetArray cache column_vector_codegen") { iter =>
        //withSQLConf(SQLConf. COLUMN_VECTOR_CODEGEN.key -> "true") { 
              DatasetArray(dsArray)
        //}
      }
      */

      benchmarkArray.run()

   }
}
