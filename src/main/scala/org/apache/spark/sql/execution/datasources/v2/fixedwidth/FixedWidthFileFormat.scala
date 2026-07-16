// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.execution.datasources.v2.fixedwidth

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

/**
 * Minimal V1 [[FileFormat]] shim for the fixed-width source.
 *
 * The fixed-width source is implemented exclusively on the DataSource V2 path.
 * [[org.apache.spark.sql.execution.datasources.v2.FileTable]] nevertheless
 * requires a V1 fallback class, which Spark consults on V1-only code paths
 * (e.g. the `FallBackFileSourceV2` rule for SQL `INSERT INTO` statements).
 * If any of those paths is ever taken, this shim fails loudly and attributably
 * instead of producing silently wrong data.
 *
 * @since 0.2.0
 */
class FixedWidthFileFormat extends FileFormat {

  private def unsupported(operation: String): Nothing =
    throw new UnsupportedOperationException(
      s"The fixedwidth-custom-scala data source does not support $operation through the " +
        "V1 FileFormat fallback. Use the DataFrame reader/writer API with format " +
        "\"fixedwidth-custom-scala\", which is served by the DataSource V2 implementation.")

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] =
    unsupported("schema inference or reading")

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    unsupported("writing")

  override def toString: String = "FixedWidth(V1 fallback shim)"
}
