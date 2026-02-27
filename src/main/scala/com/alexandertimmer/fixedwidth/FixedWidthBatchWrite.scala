// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
 * Batch write implementation for fixed-width data source.
 * Manages the writing of fixed-width files across partitions.
 */
case class FixedWidthBatchWrite(
  schema: StructType,
  options: CaseInsensitiveStringMap,
  info: LogicalWriteInfo,
  overwrite: Boolean
) extends BatchWrite {

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    // If overwrite mode and output path exists, clean it
    if (overwrite) {
      val outputPath = options.get(FixedWidthConstants.OptionKeys.PATH)
      if (outputPath != null) {
        val path = new Path(outputPath)
        val hadoopConf = SparkContext.getOrCreate().hadoopConfiguration
        val fs = path.getFileSystem(hadoopConf)
        if (fs.exists(path)) {
          fs.delete(path, true)
        }
      }
    }

    // Extract serializable options map to avoid serializing CaseInsensitiveStringMap
    val optionsMap: Map[String, String] = options.asScala.toMap
    FixedWidthDataWriterFactory(schema, optionsMap, info.queryId())
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // All files are written directly; nothing to commit
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // Could clean up partial files here, but for simplicity we rely on Spark's job cleanup
  }
}

/**
 * Factory for creating data writers for each partition.
 * Uses serializable Map[String, String] instead of CaseInsensitiveStringMap.
 */
case class FixedWidthDataWriterFactory(
  schema: StructType,
  options: Map[String, String],
  queryId: String
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    FixedWidthDataWriter(schema, options, partitionId, taskId, queryId)
  }
}
