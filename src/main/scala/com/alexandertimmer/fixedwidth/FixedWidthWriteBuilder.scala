// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Write builder for fixed-width data source.
 * Creates the batch write implementation for writing fixed-width files.
 * Supports overwrite mode for file-based writes.
 */
case class FixedWidthWriteBuilder(
  schema: StructType,
  options: CaseInsensitiveStringMap,
  info: LogicalWriteInfo
) extends WriteBuilder with SupportsTruncate with SupportsOverwrite {

  private var overwriteMode: Boolean = false

  override def truncate(): WriteBuilder = {
    overwriteMode = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    // For file-based sources, overwrite replaces all data
    overwriteMode = true
    this
  }

  override def build(): Write = FixedWidthWrite(schema, options, info, overwriteMode)
}

/**
 * Write implementation that provides batch write support.
 */
case class FixedWidthWrite(
  schema: StructType,
  options: CaseInsensitiveStringMap,
  info: LogicalWriteInfo,
  overwrite: Boolean
) extends Write {

  override def toBatch: BatchWrite = FixedWidthBatchWrite(schema, options, info, overwrite)
}
