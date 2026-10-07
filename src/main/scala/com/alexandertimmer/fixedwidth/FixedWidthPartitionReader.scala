// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, LineRecordReader}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.nio.charset.Charset

/**
 * Partition reader for fixed-width formatted files.
 *
 * This reader processes a byte range of a fixed-width file, parsing each line
 * according to the configured field positions and schema. It handles:
 *
 *  - '''Byte-based Partitioning''': Processes the `PartitionedFile` range `[startByte,
 *    startByte + lengthBytes)` with byte-exact boundaries for every line ending
 *  - '''Line Reading''': Delegated to Hadoop's `LineRecordReader`: CR, LF and CRLF are
 *    auto-detected (or an explicit `lineSep` delimiter is used), split ownership follows the
 *    standard Hadoop protocol (discard the leading partial record when `start > 0`, read one
 *    record past `end`), and codecs are handled natively (whole-file gz, block-aligned bz2)
 *  - '''Error Handling''': Supports PERMISSIVE, DROPMALFORMED, and FAILFAST modes
 *  - '''Special Columns''': Populates `_corrupt_record` and `_rescued_data` columns
 *  - '''Type Conversion''': Casts parsed strings to schema-defined types
 *
 * ==Parse Mode Behavior==
 * {{{
 * PERMISSIVE (default):
 *   - Parse errors populate rescuedDataColumn with JSON containing malformed data
 *   - Row is emitted with null values for failed fields
 *   - Short/long rows are handled gracefully
 *
 * DROPMALFORMED:
 *   - Rows with parse errors are silently dropped
 *   - Warnings logged for dropped rows
 *
 * FAILFAST:
 *   - First parse error throws SparkException
 *   - Job fails immediately
 * }}}
 *
 * @param pathStr file path to read
 * @param startByte starting byte position
 * @param lengthBytes number of bytes to read
 * @param isFirstSplit true if this is the first split of the file
 * @param schema resolved schema including special columns
 * @param fieldLengths field positions in "start:end,start:end,..." format
 * @param mode parse mode (PERMISSIVE, DROPMALFORMED, FAILFAST)
 * @param skipLines number of lines to skip at file start
 * @param encoding character encoding
 * @param rescuedDataColumn optional column name for rescued data
 * @param columnNameOfCorruptRecord optional column name for corrupt records
 * @param ignoreLeadingWhiteSpace trim leading whitespace
 * @param ignoreTrailingWhiteSpace trim trailing whitespace
 * @param nullValue string representing null
 * @param dateFormat date parsing format
 * @param timestampFormat timestamp parsing format
 * @param timeZone timezone for parsing
 * @param comment comment line indicator
 * @param lineSep optional explicit record delimiter (encoded with `encoding`); None = auto-detect
 * @since 0.1.0
 */
class FixedWidthPartitionReader(
    pathStr: String,
    startByte: Long,
    lengthBytes: Long,
    isFirstSplit: Boolean,
    schema: StructType,
    fieldLengths: String,
    mode: String,
    skipLines: Int,
    encoding: String,
    rescuedDataColumn: Option[String],
    columnNameOfCorruptRecord: Option[String],
    ignoreLeadingWhiteSpace: Boolean = true,
    ignoreTrailingWhiteSpace: Boolean = true,
    nullValue: Option[String] = None,
    dateFormat: Option[String] = None,
    timestampFormat: Option[String] = None,
    timeZone: Option[String] = None,
    comment: Option[Char] = None,
    hadoopConf: Configuration,
    includeFilePathInRescuedData: Boolean = true,
    emptyValue: Option[String] = None,
    nanValue: String = "NaN",
    positiveInf: String = "Inf",
    negativeInf: String = "-Inf",
    lineSep: Option[String] = None
) extends PartitionReader[InternalRow] {

  private val charset: Charset = Charset.forName(encoding)

  // Hadoop's LineRecordReader owns everything that used to be hand-rolled here:
  // file open + seek, codec detection (whole file for gz, block-aligned splits for
  // bz2), split-boundary record ownership (the leading partial record is discarded
  // when startByte > 0, one extra record is read past the split end), CR/LF/CRLF
  // detection (default) or an explicit `lineSep` delimiter, and UTF-8 BOM skipping.
  // This replaces the former `bytesRead += line.getBytes(encoding).length + 1`
  // accounting, which undercounted CRLF lines by one byte and made splits overlap.
  private val recordReader: LineRecordReader = {
    val split = new FileSplit(new Path(pathStr), startByte, lengthBytes, Array.empty[String])
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val context = new TaskAttemptContextImpl(hadoopConf, attemptId)
    val rr = lineSep match {
      case Some(sep) => new LineRecordReader(FWUtils.encodeLineSep(sep, charset))
      case None      => new LineRecordReader()
    }
    rr.initialize(split, context)
    rr
  }

  // Header / skip_lines: only the split starting at byte 0 contains the header.
  // LineRecordReader has already discarded the leading partial record when startByte > 0.
  if (isFirstSplit) {
    var skipped = 0
    while (skipped < skipLines && recordReader.nextKeyValue()) skipped += 1
  }

  // CSV default behavior for auto-detection:
  // - _corrupt_record: Auto-detected if column exists in schema (no explicit option needed)
  // - _rescued_data: NOT auto-detected; requires explicit rescuedDataColumn option
  // This asymmetry is documented CSV behavior per Databricks testing.
  private val DefaultCorruptCol = "_corrupt_record"

  // CRITICAL: Only _corrupt_record is auto-detected from schema.
  // _rescued_data requires explicit rescuedDataColumn option (no auto-detect).
  private val rescuedCol: Option[String] = rescuedDataColumn  // No auto-detect per CSV behavior
  private val corruptCol: Option[String] = columnNameOfCorruptRecord.orElse(
    if (schema.fieldNames.contains(DefaultCorruptCol)) Some(DefaultCorruptCol) else None
  )

  private val positions = FWUtils.parsePositionsFromString(fieldLengths)

  // Build mapping from parsed field index to schema index
  // This handles duplicate column names correctly (by position, not name)
  private val dataFieldIndices: Array[Int] = schema.fields.zipWithIndex
    .filterNot { case (f, _) => FWUtils.isSpecial(f.name, rescuedCol, corruptCol) }
    .map(_._2)

  // Get the actual field definitions for casting (in order)
  private val dataFields: Array[StructField] = dataFieldIndices.map(schema.fields(_))

  // Built once per partition: date/timestamp formatters and zone are expensive to construct
  private val caster = new FWUtils.FieldCaster(
    dataFields, dateFormat, timestampFormat, timeZone, nanValue, positiveInf, negativeInf)

  // ---- Per-reader precomputation (pre-0.2.2 these were recomputed per row / per field) ----

  /** Trim strategy resolved once. (true,true) keeps String.trim (chars <= ' '), exactly as before. */
  private val trimFn: String => String = (ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace) match {
    case (true, true)   => (s: String) => s.trim
    case (true, false)  => FWUtils.stripLeadingSpace
    case (false, true)  => FWUtils.stripTrailingSpace
    case (false, false) => identity[String]
  }

  /** Which data fields are StringType (need UTF8String wrapping). */
  private val isStringField: Array[Boolean] = dataFields.map(_.dataType == org.apache.spark.sql.types.StringType)

  private def findColIndex(name: String): Option[Int] =
    schema.fieldNames.zipWithIndex.find(_._1 == name).map(_._2)

  private val corruptColIdx: Option[Int] = corruptCol.flatMap(findColIndex)
  private val rescuedColIdx: Option[Int] = rescuedCol.flatMap(findColIndex)

  private var nextRow: Option[InternalRow] = fetchNext()

  override def next(): Boolean = nextRow.isDefined

  override def get(): InternalRow = {
    val r = nextRow.get
    nextRow = fetchNext()
    r
  }

  override def close(): Unit = recordReader.close() // closes the stream, returns the decompressor to the pool

  /**
   * Read the next line of this split. Returns None at the end of the split.
   * Split boundaries, line endings and compression are handled by LineRecordReader.
   */
  private def readNextLine(): Option[String] = {
    if (!recordReader.nextKeyValue()) {
      None
    } else {
      val text: Text = recordReader.getCurrentValue // reused buffer: copy out immediately
      val line = new String(text.getBytes, 0, text.getLength, charset)
      // Apply comment filtering
      comment match {
        case Some(c) if line.nonEmpty && line.charAt(0) == c =>
          readNextLine() // Skip comment line, try next
        case _ =>
          Some(line)
      }
    }
  }

  /**
   * Extract and trim values from a fixed-width line.
   *
   * @param line raw input line
   * @return array of extracted values (possibly null for nullValue matches)
   */
  private def extractAndTrimValues(line: String): Array[String] = {
    val out = new Array[String](positions.length)
    var i = 0
    while (i < positions.length) {
      val (s, e) = positions(i)
      val raw = if (s >= line.length) "" else line.substring(s, math.min(e, line.length))
      val trimmed = trimFn(raw)
      out(i) = nullValue match {
        case Some(nv) if trimmed == nv => null
        case _ =>
          emptyValue match {
            case Some(ev) if trimmed.isEmpty => ev
            case _ => trimmed
          }
      }
      i += 1
    }
    out
  }

  /**
   * Build the data portion of the output row.
   *
   * @param casted array of casted values
   * @param buf output buffer to populate
   */
  private def populateDataFields(casted: Array[Any], buf: Array[Any]): Unit = {
    var i = 0
    while (i < dataFieldIndices.length) {
      val v = casted(i)
      buf(dataFieldIndices(i)) =
        if (isStringField(i) && v != null) UTF8String.fromString(v.asInstanceOf[String]) else v
      i += 1
    }
  }

  /**
   * Populate special columns (_corrupt_record, _rescued_data).
   *
   * @param parsed raw parsed values
   * @param badIndices indices of fields that failed type conversion
   * @param line original input line
   * @param isStructurallyCorrupt true if row was too short
   * @param buf output buffer to populate
   */
  private def populateSpecialColumns(
      parsed: Array[String],
      badIndices: Array[Int],
      line: String,
      isStructurallyCorrupt: Boolean,
      buf: Array[Any]
  ): Unit = {
    val needsRescue = isStructurallyCorrupt || badIndices.nonEmpty

    // Corrupt column: populated when there's corruption AND rescued column is NOT set
    corruptColIdx.foreach { idx =>
      buf(idx) =
        if (needsRescue) {
          // CSV-like behavior: if rescued column exists, corrupt stays NULL
          if (rescuedColIdx.isDefined) null
          else UTF8String.fromString(parsed.mkString(","))
        } else null
    }

    // Rescued column: captures type conversion failures as JSON
    rescuedColIdx.foreach { idx =>
      if (needsRescue && badIndices.nonEmpty) {
        buf(idx) = UTF8String.fromString(
          FWUtils.buildRescuedDataFromBadIndices(parsed, badIndices, dataFields, pathStr, includeFilePathInRescuedData))
      } else if (isStructurallyCorrupt) {
        // Structural corruption without type failures - capture truncated fields
        val truncatedIndices = dataFields.indices.filter { i =>
          i < positions.length && {
            val (start, end) = positions(i)
            start >= line.length || end > line.length
          }
        }.toArray
        if (truncatedIndices.nonEmpty) {
          buf(idx) = UTF8String.fromString(
            FWUtils.buildRescuedDataFromBadIndices(parsed, truncatedIndices, dataFields, pathStr, includeFilePathInRescuedData))
        } else {
          buf(idx) = null
        }
      } else {
        buf(idx) = null
      }
    }
  }

  private def fetchNext(): Option[InternalRow] = {
    var lineOpt = readNextLine()

    while (lineOpt.isDefined) {
      val line = lineOpt.get
      val isStructurallyCorrupt = FWUtils.isStructurallyCorrupt(line, positions)

      // FAILFAST: throw on structural corruption
      if (isStructurallyCorrupt && mode == "FAILFAST") {
        throw new IllegalArgumentException(s"Structurally malformed record (row too short): $line")
      }

      // DROPMALFORMED: skip structurally corrupt rows
      if (isStructurallyCorrupt && mode == "DROPMALFORMED") {
        lineOpt = readNextLine()
      } else {
        // Extract and trim values using helper method
        val parsed = extractAndTrimValues(line)

        // Cast values and track failures
        val (casted, badIndices) = caster.cast(parsed)
        val hasTypeConversionFailure = badIndices.nonEmpty

        // FAILFAST: throw on type conversion failures
        if (hasTypeConversionFailure && mode == "FAILFAST") {
          throw new IllegalArgumentException(s"Type conversion failed for record: $line")
        }

        // DROPMALFORMED: skip rows with type conversion failures
        if (hasTypeConversionFailure && mode == "DROPMALFORMED") {
          lineOpt = readNextLine()
        } else {
          // PERMISSIVE mode (or valid row): build output row
          val buf = Array.fill[Any](schema.length)(null)

          // Populate data fields using helper method
          populateDataFields(casted, buf)

          // Populate special columns using helper method
          populateSpecialColumns(parsed, badIndices, line, isStructurallyCorrupt, buf)

          return Some(new GenericInternalRow(buf.asInstanceOf[Array[Any]]))
        }
      }
    }

    None
  }
}
