// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

/**
 * Table implementation for the fixed-width data source.
 *
 * This class implements Spark's `Table` interface with support for both
 * batch reading and batch writing of fixed-width formatted files.
 *
 * ==Capabilities==
 *  - BATCH_READ: Supports reading fixed-width files
 *  - BATCH_WRITE: Supports writing fixed-width files
 *  - ACCEPT_ANY_SCHEMA: Accepts user-provided schemas
 *  - TRUNCATE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC: Write mode support
 *
 * @param schema resolved schema including any special columns
 * @param options data source options from the read/write configuration
 * @since 0.1.0
 */
case class FixedWidthTable(schema: StructType,
                           options: org.apache.spark.sql.util.CaseInsensitiveStringMap)
  extends Table
    with SupportsRead
    with SupportsWrite {

  /** @return descriptive name for this table */
  override def name(): String = "fixedwidth-table"

  /** @return set of table capabilities supported by this implementation */
  override def capabilities(): java.util.Set[TableCapability] =
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA,
      TableCapability.TRUNCATE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC
    ).asJava

  /**
   * Creates a new scan builder for reading fixed-width files.
   *
   * @param options scan options (note: uses table options, not scan options)
   * @return scan builder for constructing the read operation
   */
  override def newScanBuilder(options: org.apache.spark.sql.util.CaseInsensitiveStringMap): ScanBuilder =
    FixedWidthScanBuilder(schema, this.options)

  /**
   * Creates a new write builder for writing fixed-width files.
   *
   * @param info logical write information including query ID and schema
   * @return write builder for constructing the write operation
   */
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    FixedWidthWriteBuilder(schema, this.options, info)
}
