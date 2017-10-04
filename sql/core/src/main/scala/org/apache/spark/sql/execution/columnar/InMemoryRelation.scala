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

package org.apache.spark.sql.execution.columnar

import org.apache.commons.lang3.StringUtils

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, ColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator


object InMemoryRelation {
  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String]): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, storageLevel, child, tableName)()

  def createColumn(cachedColumnarBatch: CachedBatch): ColumnarBatch = {
    val rowCount = cachedColumnarBatch.numRows
    val schema = cachedColumnarBatch.schema
    val columnVectors = OnHeapColumnVector.allocateColumns(rowCount, schema)
    val columnarBatch = new ColumnarBatch(
      schema, columnVectors.asInstanceOf[Array[ColumnVector]], rowCount)
    columnarBatch.setNumRows(rowCount)
    return columnarBatch
  }
}


/**
 * CachedBatch is a cached batch of rows.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 * @param schema The schema of columns
 */
private[columnar]
case class CachedBatch(
  numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow, schema: StructType)

case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    @transient child: SparkPlan,
    tableName: Option[String])(
    @transient var _cachedColumnBuffers: RDD[CachedBatch] = null,
    val batchStats: LongAccumulator = child.sqlContext.sparkContext.longAccumulator)
  extends logical.LeafNode with MultiInstanceRelation {

  /**
   * If true, get data from ColumnVector in ColumnarBatch, which are generally faster.
   * If false, get data from UnsafeRow build from ColumnVector
   */
  private[columnar] val useColumnarBatches: Boolean = {
    // In the initial implementation, for ease of review
    // support only primitive data types and # of fields is less than wholeStageMaxNumFields
    val schema = StructType.fromAttributes(child.output)
    schema.fields.find(f => f.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
            FloatType | DoubleType => false
      case _ => true
    }).isEmpty &&
      !WholeStageCodegenExec.isTooManyFields(conf, child.schema) &&
      children.find(p => WholeStageCodegenExec.isTooManyFields(conf, p.schema)).isEmpty
  }

  override protected def innerChildren: Seq[SparkPlan] = Seq(child)

  override def producedAttributes: AttributeSet = outputSet

  @transient val partitionStatistics = new PartitionStatistics(output)

  override def computeStats(): Statistics = {
    if (batchStats.value == 0L) {
      // Underlying columnar RDD hasn't been materialized, no useful statistics information
      // available, return the default statistics.
      Statistics(sizeInBytes = child.sqlContext.conf.defaultSizeInBytes)
    } else {
      Statistics(sizeInBytes = batchStats.value.longValue)
    }
  }

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    buildBuffers()
  }

  private def buildBuffers(): Unit = {
    val output = child.output
    val useColumnarBatch = useColumnarBatches
    val cached = child.execute().mapPartitionsInternal { rowIterator =>
      new Iterator[CachedBatch] {
        def next(): CachedBatch = {
          val columnBuilders = output.map { attribute =>
            ColumnBuilder(attribute.dataType, batchSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          var totalSize = 0L
          while (rowIterator.hasNext && rowCount < batchSize
            && totalSize < ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE) {
            val row = rowIterator.next()

            // Added for SPARK-6082. This assertion can be useful for scenarios when something
            // like Hive TRANSFORM is used. The external data generation script used in TRANSFORM
            // may result malformed rows, causing ArrayIndexOutOfBoundsException, which is somewhat
            // hard to decipher.
            assert(
              row.numFields == columnBuilders.length,
              s"Row column number mismatch, expected ${output.size} columns, " +
                s"but got ${row.numFields}." +
                s"\nRow content: $row")

            var i = 0
            totalSize = 0
            while (i < row.numFields) {
              columnBuilders(i).appendFrom(row, i)
              totalSize += columnBuilders(i).columnStats.sizeInBytes
              i += 1
            }
            rowCount += 1
          }

          batchStats.add(totalSize)

          val stats = InternalRow.fromSeq(
            columnBuilders.flatMap(_.columnStats.collectedStatistics))
          CachedBatch(rowCount, columnBuilders.map { builder =>
            JavaUtils.bufferToArray(builder.build())
          }, stats,
          if (useColumnarBatch) StructType.fromAttributes(output) else null)
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }.persist(storageLevel)

    cached.setName(
      tableName.map(n => s"In-memory table $n")
        .getOrElse(StringUtils.abbreviate(child.toString, 1024)))
    _cachedColumnBuffers = cached
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    InMemoryRelation(
      newOutput, useCompression, batchSize, storageLevel, child, tableName)(
        _cachedColumnBuffers, batchStats)
  }

  override def newInstance(): this.type = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName)(
        _cachedColumnBuffers,
        batchStats).asInstanceOf[this.type]
  }

  def cachedColumnBuffers: RDD[CachedBatch] = _cachedColumnBuffers

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(_cachedColumnBuffers, batchStats)
}
