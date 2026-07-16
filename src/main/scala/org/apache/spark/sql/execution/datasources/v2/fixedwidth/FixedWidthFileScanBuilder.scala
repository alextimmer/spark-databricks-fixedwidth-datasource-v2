// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.execution.datasources.v2.fixedwidth

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * [[FileScanBuilder]] for the fixed-width source.
 *
 * @since 0.2.0
 */
case class FixedWidthFileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  private var metadataRequested = false

  /**
   * Fixed-width parsing is positional over the whole line: the resolved
   * schema's field order maps 1:1 onto the configured field positions, so
   * pruning data columns would break that mapping (the pre-migration scan
   * never pruned either — Spark projects required columns above the scan).
   * Only partition-column pruning and the `_metadata` request are honored.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    val resolver = sparkSession.sessionState.conf.resolver
    val requestedPartitionFields = fileIndex.partitionSchema.fields.filter { p =>
      requiredSchema.fields.exists(r => resolver(r.name, p.name))
    }
    metadataRequested = requiredSchema.fields.exists { r =>
      resolver(r.name, FixedWidthFileTable.METADATA_COL_NAME)
    }
    this.requiredSchema = StructType(dataSchema.fields ++ requestedPartitionFields)
  }

  override def build(): Scan = {
    val readMetadataSchema = if (metadataRequested) {
      StructType(Seq(org.apache.spark.sql.types.StructField(
        FixedWidthFileTable.METADATA_COL_NAME,
        FixedWidthFileTable.METADATA_STRUCT,
        nullable = false)))
    } else {
      new StructType()
    }
    FixedWidthFileScan(sparkSession, fileIndex, dataSchema, readDataSchema(),
      readPartitionSchema(), options, readMetadataSchema, partitionFilters, dataFilters)
  }
}
