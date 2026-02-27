// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

import FixedWidthConstants._
import FixedWidthConstants.{ErrorMessages => Errors}
import FixedWidthConstants.{OptionKeys => Keys}

/**
 * Utility functions for the fixed-width data source.
 *
 * This object provides:
 *  - '''Input Validation''': Parse and validate configuration options
 *  - '''Schema Operations''': Infer schemas and append special columns
 *  - '''Field Parsing''': Extract and trim field values from raw lines
 *  - '''Type Conversion''': Cast string values to appropriate Spark types
 *  - '''Error Handling''': Generate rescued data JSON and corrupt record strings
 *
 * ==Input Validation==
 * All input validation methods throw `IllegalArgumentException` with clear
 * error messages for invalid values. Methods include:
 *  - `parseSkipLines`: Validate skip_lines >= 0
 *  - `parseMaxPartitionBytes`: Validate positive byte count
 *  - `parseNumPartitions`: Validate positive partition count
 *  - `validateAndParseFieldLengths`: Validate field_lengths format
 *
 * ==Schema Handling==
 * The `appendSpecialColumns` method conditionally adds special columns
 * (`columnNameOfCorruptRecord`, `rescuedDataColumn`) to schemas if:
 *  - The option is configured
 *  - The column doesn't already exist in the schema
 *
 * ==Type Conversion==
 * The `castValue` method converts string values to Spark types with support for:
 *  - Numeric types (IntegerType, LongType, FloatType, DoubleType, DecimalType)
 *  - Date/Time types (DateType, TimestampType) with configurable formats
 *  - Boolean types with case-insensitive parsing
 *  - Null value handling via configurable null string
 *
 * @since 0.1.0
 */
object FWUtils {

  // =====================================================
  // INPUT VALIDATION HELPERS
  // =====================================================

  /**
   * Parse and validate skip_lines option.
   * Returns 0 if not specified, throws clear error for invalid values.
   */
  def parseSkipLines(opts: CaseInsensitiveStringMap): Int = {
    Option(opts.get(Keys.SKIP_LINES)) match {
      case None => DEFAULT_SKIP_LINES
      case Some(value) =>
        Try(value.toInt) match {
          case Success(n) if n >= 0 => n
          case Success(n) =>
            throw new IllegalArgumentException(Errors.negativeSkipLines(n))
          case Failure(_) =>
            throw new IllegalArgumentException(Errors.invalidSkipLines(value))
        }
    }
  }

  /**
   * Parse and validate maxPartitionBytes option.
   * Returns the default (128MB) if not specified.
   */
  def parseMaxPartitionBytes(opts: CaseInsensitiveStringMap, default: Long = DEFAULT_MAX_PARTITION_BYTES): Long = {
    Option(opts.get(Keys.MAX_PARTITION_BYTES)) match {
      case None => default
      case Some(value) =>
        Try(value.toLong) match {
          case Success(n) if n > 0 => n
          case Success(n) =>
            throw new IllegalArgumentException(
              s"maxPartitionBytes must be positive, got: $n")
          case Failure(_) =>
            throw new IllegalArgumentException(Errors.invalidMaxPartitionBytes(value))
        }
    }
  }

  /**
   * Parse and validate numPartitions option.
   * Returns None if not specified.
   */
  def parseNumPartitions(opts: CaseInsensitiveStringMap): Option[Int] = {
    Option(opts.get(Keys.NUM_PARTITIONS)).map { value =>
      Try(value.toInt) match {
        case Success(n) if n > 0 => n
        case Success(n) =>
          throw new IllegalArgumentException(
            s"numPartitions must be positive, got: $n")
        case Failure(_) =>
          throw new IllegalArgumentException(Errors.invalidNumPartitions(value))
      }
    }
  }

  /**
   * Validate field_lengths format and return parsed positions.
   * Validates:
   * - Correct "start:end" format
   * - start < end for each range
   * - No overlapping ranges
   * - Non-negative values
   */
  def validateAndParseFieldLengths(fieldLengths: String): Array[(Int, Int)] = {
    val positions = parsePositionsFromString(fieldLengths)
    validatePositions(positions)
    positions
  }

  /**
   * Validate position array for logical consistency.
   */
  private def validatePositions(positions: Array[(Int, Int)]): Unit = {
    positions.zipWithIndex.foreach { case ((start, end), idx) =>
      if (start < 0) {
        throw new IllegalArgumentException(
          s"field_lengths range $idx has negative start position: $start")
      }
      if (end < 0) {
        throw new IllegalArgumentException(
          s"field_lengths range $idx has negative end position: $end")
      }
      if (start >= end) {
        throw new IllegalArgumentException(Errors.invalidRange(start, end))
      }
    }

    // Check for overlapping ranges
    val sorted = positions.sortBy(_._1)
    for (i <- 0 until sorted.length - 1) {
      val (_, end1) = sorted(i)
      val (start2, _) = sorted(i + 1)
      if (end1 > start2) {
        throw new IllegalArgumentException(Errors.overlappingRanges(sorted(i), sorted(i + 1)))
      }
    }
  }

  def parsePositions(opts: CaseInsensitiveStringMap): Array[(Int, Int)] = {
    // field_simple takes precedence, then field_lengths, then schema metadata (if schema provided)
    val fieldSimple = Option(opts.get(Keys.FIELD_SIMPLE))
    val fieldLengths = Option(opts.get(Keys.FIELD_LENGTHS))

    (fieldSimple, fieldLengths) match {
      case (Some(simple), _) => parseFieldSimple(simple)
      case (None, Some(lengths)) => validateAndParseFieldLengths(lengths)
      case _ => throw new IllegalArgumentException(Errors.missingFieldLengths)
    }
  }

  /**
   * Parse positions with schema metadata fallback.
   * Priority: field_simple > field_lengths > schema metadata widths
   */
  def parsePositionsWithSchema(opts: CaseInsensitiveStringMap, schema: StructType): Array[(Int, Int)] = {
    val fieldSimple = Option(opts.get(Keys.FIELD_SIMPLE))
    val fieldLengths = Option(opts.get(Keys.FIELD_LENGTHS))

    (fieldSimple, fieldLengths) match {
      case (Some(simple), _) => parseFieldSimple(simple)
      case (None, Some(lengths)) => validateAndParseFieldLengths(lengths)
      case _ =>
        // Try extracting widths from schema metadata
        extractWidthsFromMetadata(schema) match {
          case Some(widths) => widthsToPositions(widths)
          case None => throw new IllegalArgumentException(Errors.missingFieldLengthsWithSchema)
        }
    }
  }

  /**
   * Extract field widths from schema metadata.
   * Each field must have a 'width' attribute in its metadata.
   * Returns None if any field is missing width metadata.
   */
  def extractWidthsFromMetadata(schema: StructType): Option[Array[Int]] = {
    val widths = schema.fields.map { field =>
      if (field.metadata.contains("width")) {
        Some(field.metadata.getLong("width").toInt)
      } else {
        None
      }
    }

    // Return Some only if ALL fields have width metadata
    if (widths.forall(_.isDefined)) {
      Some(widths.map(_.get))
    } else {
      None
    }
  }

  /**
   * Convert array of widths to array of (start, end) positions.
   */
  def widthsToPositions(widths: Array[Int]): Array[(Int, Int)] = {
    var pos = 0
    widths.map { width =>
      val start = pos
      pos += width
      (start, pos)
    }
  }

  /**
   * Parse field_simple format: "5,5,1" means widths of 5, 5, 1 starting at position 0.
   * Converts to position tuples: [(0,5), (5,10), (10,11)]
   */
  def parseFieldSimple(fieldSimple: String): Array[(Int, Int)] = {
    val widths = fieldSimple.split(",").map(_.trim.toInt)
    widthsToPositions(widths)
  }

  def parsePositionsFromString(fieldLengths: String): Array[(Int, Int)] = {
    // Validate format uses colon separator, not hyphen
    if (fieldLengths.contains("-") && !fieldLengths.contains(":")) {
      throw new IllegalArgumentException(
        s"Invalid field_lengths format: '$fieldLengths'. Use colon separator (e.g., '0:5,5:10'), not hyphen.")
    }

    val positions = fieldLengths.split(",").map { part =>
      val arr = part.split(":")
      if (arr.length != 2) {
        throw new IllegalArgumentException(
          s"Invalid field_lengths segment: '$part'. Each segment must be 'start:end' format.")
      }

      val start = try {
        arr(0).trim.toInt
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(
            s"Invalid field_lengths range start: '${arr(0)}' is not a valid integer.")
      }

      val end = try {
        arr(1).trim.toInt
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(
            s"Invalid field_lengths range end: '${arr(1)}' is not a valid integer.")
      }

      if (start > end) {
        throw new IllegalArgumentException(
          s"Invalid field_lengths range: start ($start) > end ($end) in segment '$part'.")
      }

      (start, end)
    }

    // Check for overlapping ranges
    val sortedPositions = positions.sortBy(_._1)
    for (i <- 0 until sortedPositions.length - 1) {
      val (_, end1) = sortedPositions(i)
      val (start2, _) = sortedPositions(i + 1)
      if (end1 > start2) {
        throw new IllegalArgumentException(
          s"Invalid field_lengths: overlapping ranges detected. " +
          s"Range ending at $end1 overlaps with range starting at $start2.")
      }
    }

    positions
  }

  def inferBaseSchema(opts: CaseInsensitiveStringMap): StructType = {
    val n = parsePositions(opts).length
    StructType((1 to n).map(i =>
      StructField(s"col$i", StringType, nullable = true)
    ))
  }

  /**
   * Appends special columns to a schema following CSV PERMISSIVE mode behavior.
   *
   * CRITICAL: CSV has DIFFERENT behaviors for these two options:
   * - rescuedDataColumn: If option set AND column NOT in schema → AUTO-APPEND column
   * - columnNameOfCorruptRecord: If option set AND column NOT in schema → SILENTLY DROP (don't add)
   *
   * This asymmetry is documented CSV behavior:
   * - rescuedDataColumn absent from schema → Spark appends automatically
   * - columnNameOfCorruptRecord absent from schema → Spark silently drops
   *
   * @param base The base schema (either user-provided or inferred from field_lengths)
   * @param opts Configuration options (may contain rescuedDataColumn, columnNameOfCorruptRecord)
   * @return Schema with rescuedDataColumn appended if needed (corrupt NOT auto-appended)
   */
  def appendSpecialColumns(base: StructType,
                           opts: CaseInsensitiveStringMap): StructType = {

    val names = base.fieldNames.toSet

    // Only auto-append rescued column if option is set AND column not already in schema
    // This replicates CSV behavior where rescuedDataColumn is auto-appended
    val rescued =
      Option(opts.get(Keys.RESCUED_DATA_COLUMN))
        .filterNot(names.contains)
        .map(StructField(_, StringType, nullable = true))

    // DO NOT auto-append corrupt column - per CSV behavior, if columnNameOfCorruptRecord
    // is set but column is NOT in user's schema, Spark silently drops it (doesn't add)
    // The corrupt column is only used if explicitly declared in the schema

    // Append only rescued column to end (if any)
    StructType(base.fields ++ rescued)
  }

  def isSpecial(name: String,
                rescued: Option[String],
                corrupt: Option[String]): Boolean =
    rescued.contains(name) || corrupt.contains(name)

  /**
   * Cast parsed string values to schema field types.
   *
   * PERMISSIVE mode behavior (replicates CSV):
   * - Type conversion failures → field set to NULL
   * - Returns indices of failed conversions for optional rescue tracking
   * - Handles schema/values length mismatch gracefully
   *
   * Whether type conversion failures trigger special column population depends
   * on whether rescuedDataColumn/columnNameOfCorruptRecord options are set.
   *
   * @param values Parsed string values from fixed-width extraction
   * @param fields Schema fields to cast to
   * @param dateFormat Optional date format pattern (default: yyyy-MM-dd)
   * @param timestampFormat Optional timestamp format pattern (default: yyyy-MM-dd HH:mm:ss)
   * @param timeZone Optional timezone ID (default: system default)
   * @return Tuple of (casted values, indices of fields that failed conversion)
   */
  def cast(values: Array[String],
           fields: Array[StructField],
           dateFormat: Option[String] = None,
           timestampFormat: Option[String] = None,
           timeZone: Option[String] = None): (Array[Any], Array[Int]) = {

    val out = new Array[Any](fields.length)
    val badIndices = scala.collection.mutable.ArrayBuffer[Int]()

    // Create formatters for date/timestamp parsing
    val dateFormatter = DateTimeFormatter.ofPattern(dateFormat.getOrElse("yyyy-MM-dd"))
    val tsFormatter = DateTimeFormatter.ofPattern(timestampFormat.getOrElse("yyyy-MM-dd HH:mm:ss"))
    val zone = ZoneId.of(timeZone.getOrElse("UTC"))

    fields.indices.foreach { i =>
      val f = fields(i)
      // Handle case where schema has more fields than parsed values
      // Also preserve null values (from nullValue option)
      val rawVal = if (i < values.length) values(i) else null

      // If input is null (from nullValue matching), output is null
      if (rawVal == null) {
        out(i) = null
      } else {
        val v = rawVal
        try {
          out(i) = f.dataType match {
            case StringType  => v
            case IntegerType => if (v.isEmpty) null else v.toInt
            case LongType    => if (v.isEmpty) null else v.toLong
            case FloatType   => if (v.isEmpty) null else v.toFloat
            case DoubleType  => if (v.isEmpty) null else v.toDouble
            case BooleanType => if (v.isEmpty) null else v.toBoolean
            case DateType =>
              if (v.isEmpty) null else {
                // Parse date and return days since epoch (Spark internal format)
                val parsedDate = LocalDate.parse(v, dateFormatter)
                ChronoUnit.DAYS.between(EPOCH_DATE, parsedDate).toInt
              }
            case TimestampType =>
              if (v.isEmpty) null else {
                // Parse timestamp and return microseconds since epoch (Spark internal format)
                val parsedTs = LocalDateTime.parse(v.trim, tsFormatter)
                val instant = parsedTs.atZone(zone).toInstant
                instant.getEpochSecond * 1000000L + instant.getNano / 1000L
              }
            case dt: DecimalType =>
              if (v.isEmpty) null else {
                Decimal(new java.math.BigDecimal(v.trim), dt.precision, dt.scale)
              }
            case _ => v
          }
        } catch {
          case NonFatal(_) =>
            out(i) = null
            badIndices += i
        }
      }
    }

    (out, badIndices.toArray)
  }

  /**
   * Check if a raw line has structural corruption for fixed-width parsing.
   *
   * Structural corruption means the row cannot be parsed at all, NOT
   * that individual fields fail type conversion (that's PERMISSIVE NULL behavior).
   *
   * For fixed-width files, structural corruption occurs when:
   * - Row is too short to extract ALL expected fields (line.length < max end position)
   *
   * Type conversion failures (e.g., "abc" → LongType) are NOT structural corruption.
   *
   * @param line Raw input line
   * @param positions Field extraction positions
   * @return true if structurally corrupt, false otherwise
   */
  def isStructurallyCorrupt(line: String, positions: Array[(Int, Int)]): Boolean = {
    if (positions.isEmpty) return false
    val maxEnd = positions.map(_._2).max
    // Row is corrupt if it's shorter than the minimum required length
    line.length < maxEnd
  }

  /**
   * Build rescued data JSON for type conversion failures.
   *
   * @param parsed Original parsed string values
   * @param badIndices Indices of fields that failed type conversion
   * @param fields Schema fields
   * @param filePath Source file path
   * @return JSON string with failed field values and file path
   */
  def buildRescuedDataFromBadIndices(parsed: Array[String],
                                     badIndices: Array[Int],
                                     fields: Array[StructField],
                                     filePath: String): String = {
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule

    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

    val rescuedMap = badIndices.flatMap { i =>
      if (i < fields.length && i < parsed.length) {
        Some(fields(i).name -> parsed(i))
      } else None
    }.toMap + ("_file_path" -> filePath)

    mapper.writeValueAsString(rescuedMap)
  }
}
