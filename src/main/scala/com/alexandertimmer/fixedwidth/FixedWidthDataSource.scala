// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Spark DataSource V2 implementation for reading fixed-width formatted text files.
 *
 * This data source replicates the exact behavior of Spark's built-in CSV reader when
 * operating in PERMISSIVE mode, including support for corrupt record handling and
 * rescued data columns.
 *
 * ==Supported Options==
 *
 * '''Required (one of):'''
 *  - `field_lengths`: Field position ranges in format "start:end,start:end,..."
 *    where positions are 0-indexed integers (e.g., "0:5,5:10,10:15")
 *  - `field_simple`: Comma-separated list of field widths (e.g., "5,5,5")
 *
 * '''Optional:'''
 *  - `columnNameOfCorruptRecord`: Column name for storing unparseable rows
 *  - `rescuedDataColumn`: Column name for storing rescued data as JSON
 *  - `skip_lines`: Number of header lines to skip (default: 0)
 *  - `encoding`: Character encoding (default: UTF-8)
 *  - `mode`: Parse mode - PERMISSIVE, DROPMALFORMED, or FAILFAST (default: PERMISSIVE)
 *  - `dateFormat`: Custom date format pattern
 *  - `timestampFormat`: Custom timestamp format pattern
 *  - `nullValue`: String to interpret as null value
 *  - `comment`: Character indicating comment lines to skip
 *
 * ==Usage Example==
 * {{{
 * val df = spark.read.format("fixedwidth-custom-scala")
 *   .option("field_lengths", "0:10,10:20,20:25")
 *   .option("columnNameOfCorruptRecord", "_corrupt_record")
 *   .schema(mySchema)
 *   .load("/path/to/fixed-width-file.txt")
 * }}}
 *
 * ==CSV PERMISSIVE Mode Compatibility==
 *
 * This implementation matches CSV PERMISSIVE behavior:
 *  - Malformed rows populate `columnNameOfCorruptRecord` with raw line
 *  - Type conversion failures populate `rescuedDataColumn` with JSON details
 *  - Well-formed rows have NULL in both special columns
 *
 * ==Schema Handling Strategy==
 *
 *  - `inferSchema()` returns base schema WITHOUT special columns
 *  - `getTable()` receives either user schema OR inferred schema
 *  - Special columns are conditionally appended for both cases
 *
 * @see [[https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/sources/DataSourceRegister.html]]
 * @since 0.1.0
 */
class FixedWidthDataSource
  extends TableProvider
    with DataSourceRegister {

  /**
   * Returns the short name used in `spark.read.format()`.
   *
   * @return the format name "fixedwidth-custom-scala"
   */
  override def shortName(): String = FixedWidthConstants.FORMAT_SHORT_NAME

  /**
   * Declares this source supports user-supplied schemas.
   *
   * @return true, indicating external metadata (user schemas) are supported
   */
  override def supportsExternalMetadata(): Boolean = true

  /**
   * Infers base schema from field_lengths option.
   *
   * Returns ONLY the data columns (col1, col2, etc.) as StringType.
   * Special columns (rescuedDataColumn, columnNameOfCorruptRecord) are
   * added later in `getTable()`.
   *
   * @param options case-insensitive map of data source options
   * @return inferred schema with one StringType column per field
   * @throws IllegalArgumentException if field_lengths or field_simple is not specified
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    FWUtils.inferBaseSchema(options)
  }

  /**
   * Creates a Table representing the fixed-width data source.
   *
   * The schema parameter is NEVER null in DataSource V2. It contains EITHER:
   *  1. User-provided schema (if `.schema(userSchema)` was called)
   *  2. Result from `inferSchema()` (if schema was not provided)
   *
   * In both cases, special columns are conditionally appended:
   *  - `rescuedDataColumn` (if option is set and column not in schema)
   *  - `columnNameOfCorruptRecord` (if option is set and column not in schema)
   *
   * @param schema user-provided or inferred schema
   * @param partitions partitioning transforms (currently unused)
   * @param properties case-insensitive map of data source options
   * @return FixedWidthTable instance for scanning the data
   */
  override def getTable(schema: StructType,
                        partitions: Array[org.apache.spark.sql.connector.expressions.Transform],
                        properties: java.util.Map[String, String]): Table = {

    val opts = new CaseInsensitiveStringMap(properties)
    val resolved = FWUtils.appendSpecialColumns(schema, opts)

    FixedWidthTable(resolved, opts)
  }
}
