// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import java.time.LocalDate

/**
 * Constants used throughout the fixed-width data source implementation.
 *
 * This object centralizes all magic numbers, strings, and configuration defaults
 * for improved maintainability and consistency. All constants are organized into
 * logical groups:
 *
 *  - '''Default Values''': Buffer sizes, encodings, partition limits
 *  - '''Parse Modes''': PERMISSIVE, DROPMALFORMED, FAILFAST
 *  - '''Option Keys''': Configuration option names (case-insensitive)
 *  - '''Error Messages''': User-facing error message templates
 *  - '''Rescued Data Fields''': JSON field names for rescued data column
 *
 * ==Usage==
 * {{{
 * import FixedWidthConstants._
 * import FixedWidthConstants.{OptionKeys => Keys}
 *
 * val mode = Option(opts.get(Keys.MODE)).getOrElse(DEFAULT_MODE)
 * val encoding = Option(opts.get(Keys.ENCODING)).getOrElse(DEFAULT_ENCODING)
 * }}}
 *
 * @since 0.1.0
 */
object FixedWidthConstants {

  // ===========================================================================
  // Default Values
  // ===========================================================================

  /** Default buffer size for file reading (8KB) */
  val DEFAULT_BUFFER_SIZE: Int = 8192

  /** Default character encoding for text files */
  val DEFAULT_ENCODING: String = "UTF-8"

  /** Default parse mode (replicating CSV PERMISSIVE behavior) */
  val DEFAULT_MODE: String = "PERMISSIVE"

  /** Default maximum partition size in bytes (128MB) */
  val DEFAULT_MAX_PARTITION_BYTES: Long = 134217728L

  /** Epoch date for date calculations (1970-01-01) */
  val EPOCH_DATE: LocalDate = LocalDate.of(1970, 1, 1)

  /** Default number of lines to skip (headers) */
  val DEFAULT_SKIP_LINES: Int = 0

  /** Default number of partitions (auto-calculated when 0) */
  val DEFAULT_NUM_PARTITIONS: Int = 0

  /** Default padding character for fixed-width fields */
  val DEFAULT_PADDING_CHAR: Char = ' '

  /** Default field alignment (left-aligned, right-padded) */
  val DEFAULT_ALIGNMENT: String = "left"

  // ===========================================================================
  // Parse Modes
  // ===========================================================================

  /** Parse modes for handling malformed records */
  object ParseModes {
    val PERMISSIVE = "PERMISSIVE"
    val DROPMALFORMED = "DROPMALFORMED"
    val FAILFAST = "FAILFAST"
  }

  // ===========================================================================
  // Option Keys
  // ===========================================================================

  /** Option key names for DataSource configuration */
  object OptionKeys {
    val FIELD_LENGTHS = "field_lengths"
    val FIELD_SIMPLE = "field_simple"
    val COLUMN_NAME_OF_CORRUPT_RECORD = "columnNameOfCorruptRecord"
    val RESCUED_DATA_COLUMN = "rescuedDataColumn"
    val SKIP_LINES = "skip_lines"
    val ENCODING = "encoding"
    val MODE = "mode"
    val DATE_FORMAT = "dateFormat"
    val TIMESTAMP_FORMAT = "timestampFormat"
    val TIME_ZONE = "timeZone"
    val NULL_VALUE = "nullValue"
    val COMMENT = "comment"
    val IGNORE_LEADING_WHITE_SPACE = "ignoreLeadingWhiteSpace"
    val IGNORE_TRAILING_WHITE_SPACE = "ignoreTrailingWhiteSpace"
    val NUM_PARTITIONS = "numPartitions"
    val MAX_PARTITION_BYTES = "maxPartitionBytes"
    val PATH = "path"
    val PADDING_CHAR = "paddingChar"
    val ALIGNMENT = "alignment"
    val LINE_ENDING = "lineEnding"
    val SCHEMA_METADATA_WIDTHS = "schema_metadata_widths"
  }

  // ===========================================================================
  // Rescued Data JSON Field Names
  // ===========================================================================

  /** JSON field names for rescued data column */
  object RescuedDataFields {
    val FILE_PATH = "_file_path"
    val FIELD_NAME = "field_name"
    val ORIGINAL_VALUE = "original_value"
    val EXPECTED_TYPE = "expected_type"
    val TRUNCATED_AT = "_truncated_at"
  }

  // ===========================================================================
  // Error Messages
  // ===========================================================================

  /** Standardized error messages for validation and parsing failures */
  object ErrorMessages {
    def missingFieldLengths: String =
      "Either 'field_simple' or 'field_lengths' option is required"

    def missingFieldLengthsWithSchema: String =
      "Either 'field_simple' or 'field_lengths' option is required, " +
        "or all schema fields must have 'width' metadata"

    def invalidFieldLengthsFormat(format: String): String =
      s"Invalid field_lengths format: '$format'. Expected: 'start:end,start:end,...' " +
        "where start and end are non-negative integers and start < end"

    def overlappingRanges(range1: (Int, Int), range2: (Int, Int)): String =
      s"field_lengths contains overlapping ranges: $range1 and $range2"

    def invalidRange(start: Int, end: Int): String =
      s"field_lengths contains invalid range: start ($start) must be less than end ($end)"

    def negativeSkipLines(value: Int): String =
      s"skip_lines must be non-negative, got: $value"

    def invalidSkipLines(value: String): String =
      s"skip_lines must be a valid integer, got: '$value'"

    def invalidNumPartitions(value: String): String =
      s"numPartitions must be a valid non-negative integer, got: '$value'"

    def invalidMaxPartitionBytes(value: String): String =
      s"maxPartitionBytes must be a valid positive integer, got: '$value'"

    def typeConversionFailed(line: String): String =
      s"Type conversion failed for record: $line"

    def structurallyMalformedRecord(line: String): String =
      s"Structurally malformed record (row too short): $line"
  }

  // ===========================================================================
  // DataSource Registration
  // ===========================================================================

  /** Short name for format registration */
  val FORMAT_SHORT_NAME: String = "fixedwidth-custom-scala"

  /** Full class name for ServiceLoader registration */
  val DATASOURCE_CLASS_NAME: String = "com.alexandertimmer.fixedwidth.DefaultSource"
}
