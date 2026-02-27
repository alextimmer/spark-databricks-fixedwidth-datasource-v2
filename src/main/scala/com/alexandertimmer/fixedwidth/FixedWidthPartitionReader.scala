// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.fs.{FSDataInputStream, Path}

/**
 * Factory for creating partition readers for fixed-width files.
 *
 * This factory is serialized to executors and creates readers with
 * consistent configuration across all partitions.
 *
 * @param schema resolved schema including special columns
 * @param fieldLengths field positions in "start:end,start:end,..." format
 * @param mode parse mode: PERMISSIVE, DROPMALFORMED, or FAILFAST
 * @param skipLines number of lines to skip at file start
 * @param encoding character encoding (default UTF-8)
 * @param rescuedDataColumn optional column name for rescued data JSON
 * @param columnNameOfCorruptRecord optional column name for corrupt records
 * @param ignoreLeadingWhiteSpace trim leading whitespace from fields
 * @param ignoreTrailingWhiteSpace trim trailing whitespace from fields
 * @param nullValue string representing null values
 * @param dateFormat date parsing format (Java DateTimeFormatter)
 * @param timestampFormat timestamp parsing format (Java DateTimeFormatter)
 * @param timeZone timezone for date/timestamp parsing
 * @param comment character indicating comment lines to skip
 * @since 0.1.0
 */
case class FixedWidthPartitionReaderFactory(
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
    comment: Option[Char] = None
) extends PartitionReaderFactory with Serializable {

  /**
   * Creates a partition reader for the given partition.
   *
   * @param partition input partition containing file path and byte range
   * @return reader for processing the partition
   */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[FixedWidthPartition]
    new FixedWidthPartitionReader(
      pathStr = p.path,
      startByte = p.start,
      lengthBytes = p.length,
      isFirstSplit = p.isFirstSplit,
      schema = schema,
      fieldLengths = fieldLengths,
      mode = mode,
      skipLines = skipLines,
      encoding = encoding,
      rescuedDataColumn = rescuedDataColumn,
      columnNameOfCorruptRecord = columnNameOfCorruptRecord,
      ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
      ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
      nullValue = nullValue,
      dateFormat = dateFormat,
      timestampFormat = timestampFormat,
      timeZone = timeZone,
      comment = comment
    )
  }
}

/**
 * Partition reader for fixed-width formatted files.
 *
 * This reader processes a byte range of a fixed-width file, parsing each line
 * according to the configured field positions and schema. It handles:
 *
 *  - '''Byte-based Partitioning''': Reads from `startByte` to `startByte + lengthBytes`,
 *    with proper handling of partial lines at partition boundaries
 *  - '''Compressed Files''': Automatically decompresses using Hadoop codecs
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
    comment: Option[Char] = None
) extends PartitionReader[InternalRow] {

  import org.apache.hadoop.io.compress.CompressionCodecFactory

  private val spark = SparkSession.active
  private val hadoopConf = spark.sparkContext.hadoopConfiguration
  private val path = new Path(pathStr)
  private val fs = path.getFileSystem(hadoopConf)

  // Check if file is compressed
  private val codecFactory = new CompressionCodecFactory(hadoopConf)
  private val codec = Option(codecFactory.getCodec(path))
  private val isCompressed = codec.isDefined

  // For compressed files, we always read from the beginning and process entire file
  // For uncompressed files, we use byte-based partitioning
  private val effectiveStart: Long = if (isCompressed) 0L else startByte
  private val effectiveLength: Long = if (isCompressed) fs.getFileStatus(path).getLen else lengthBytes
  private val effectiveIsFirstSplit: Boolean = if (isCompressed) true else isFirstSplit

  private val stream: FSDataInputStream = fs.open(path)

  // Track bytes read to know when to stop for this partition
  private var bytesRead: Long = 0
  private val endPosition: Long = effectiveStart + effectiveLength

  // Create input stream (potentially decompressed)
  private val inputStream: java.io.InputStream = codec match {
    case Some(c) => c.createInputStream(stream)
    case None =>
      // Seek to start position and handle alignment for non-first splits
      val adjustedStart = if (!effectiveIsFirstSplit && effectiveStart > 0) {
        // Check if previous byte was a newline (meaning we're at line start)
        stream.seek(effectiveStart - 1)
        val prevByte = stream.read()
        if (prevByte == '\n') {
          effectiveStart
        } else {
          // We're mid-line, skip to next line
          stream.seek(effectiveStart)
          val tempReader = new java.io.BufferedReader(new java.io.InputStreamReader(stream, encoding))
          val partialLine = tempReader.readLine()
          if (partialLine != null) {
            bytesRead += partialLine.getBytes(encoding).length + 1
          }
          effectiveStart + bytesRead
        }
      } else {
        effectiveStart
      }

      stream.seek(adjustedStart)
      stream
  }

  private val reader = new java.io.BufferedReader(
    new java.io.InputStreamReader(inputStream, encoding)
  )

  // Skip header lines only on first partition (or for compressed files)
  private val linesToSkip = if (effectiveIsFirstSplit) skipLines else 0
  for (_ <- 0 until linesToSkip) {
    val skippedLine = reader.readLine()
    if (skippedLine != null) {
      bytesRead += skippedLine.getBytes(encoding).length + 1
    }
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

  private var nextRow: Option[InternalRow] = fetchNext()

  override def next(): Boolean = nextRow.isDefined

  override def get(): InternalRow = {
    val r = nextRow.get
    nextRow = fetchNext()
    r
  }

  override def close(): Unit = {
    reader.close()
    stream.close()
  }

  /**
   * Read the next line from the file, respecting partition byte boundaries.
   * Returns None when we've passed the end of this partition's byte range.
   * For compressed files, reads until EOF (no byte-based splitting).
   */
  private def readNextLine(): Option[String] = {
    // For compressed files, we read until EOF (no byte-based boundary checking)
    // For uncompressed files, check if we've read past our partition's byte range
    if (!isCompressed) {
      if (effectiveStart + bytesRead >= endPosition) {
        return None
      }
    }

    val line = reader.readLine()
    if (line == null) {
      None
    } else {
      if (!isCompressed) {
        bytesRead += line.getBytes(encoding).length + 1  // +1 for newline
      }
      // Apply comment filtering
      comment match {
        case Some(c) if line.nonEmpty && line.charAt(0) == c =>
          readNextLine()  // Skip comment line, try next
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
    positions.map { case (s, e) =>
      val raw = if (s >= line.length) "" else line.substring(s, math.min(e, line.length))
      // Apply conditional trimming
      val trimmed = (ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace) match {
        case (true, true) => raw.trim
        case (true, false) => raw.replaceAll("^\\s+", "")
        case (false, true) => raw.replaceAll("\\s+$", "")
        case (false, false) => raw
      }
      // Apply nullValue check
      nullValue match {
        case Some(nv) if trimmed == nv => null
        case _ => trimmed
      }
    }
  }

  /**
   * Find the schema index for a column name.
   */
  private def findColIndex(name: String): Option[Int] =
    schema.fieldNames.zipWithIndex.find(_._1 == name).map(_._2)

  /**
   * Build the data portion of the output row.
   *
   * @param casted array of casted values
   * @param buf output buffer to populate
   */
  private def populateDataFields(casted: Array[Any], buf: Array[Any]): Unit = {
    dataFieldIndices.indices.foreach { i =>
      val schemaIdx = dataFieldIndices(i)
      buf(schemaIdx) = (casted(i), dataFields(i).dataType) match {
        case (s: String, _: org.apache.spark.sql.types.StringType) => UTF8String.fromString(s)
        case (v, _) => v
      }
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
    corruptCol.foreach { col =>
      findColIndex(col).foreach { idx =>
        buf(idx) =
          if (needsRescue) {
            // CSV-like behavior: if rescued column exists, corrupt stays NULL
            if (rescuedCol.flatMap(findColIndex).isDefined) null
            else UTF8String.fromString(parsed.mkString(","))
          } else null
      }
    }

    // Rescued column: captures type conversion failures as JSON
    rescuedCol.foreach { col =>
      findColIndex(col).foreach { idx =>
        if (needsRescue && badIndices.nonEmpty) {
          buf(idx) = UTF8String.fromString(
            FWUtils.buildRescuedDataFromBadIndices(parsed, badIndices, dataFields, pathStr)
          )
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
              FWUtils.buildRescuedDataFromBadIndices(parsed, truncatedIndices, dataFields, pathStr)
            )
          } else {
            buf(idx) = null
          }
        } else {
          buf(idx) = null
        }
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
        val (casted, badIndices) = FWUtils.cast(parsed, dataFields, dateFormat, timestampFormat, timeZone)
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
