// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types._

import FixedWidthConstants.{OptionKeys => Keys}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.Charset
import java.time.{Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/**
 * Data writer for fixed-width format.
 * Each writer handles one partition, writing rows to a single output file.
 */
case class FixedWidthDataWriter(
  schema: StructType,
  options: Map[String, String],
  partitionId: Int,
  taskId: Long,
  queryId: String
) extends DataWriter[InternalRow] {

  // Normalize option keys to lowercase for consistent lookup.
  // Spark's CaseInsensitiveMap lowercases keys before they reach the writer
  // via Map[String, String] (which is case-sensitive), so we must match.
  private val opts: Map[String, String] = options.map { case (k, v) =>
    k.toLowerCase(java.util.Locale.ROOT) -> v
  }

  // Parse field widths from options
  private val fieldWidths: Array[Int] = parseFieldWidths()

  private val outputPath: String = opts.getOrElse(Keys.PATH,
    throw new IllegalArgumentException("'path' option is required for writing"))
  private val encoding: String = opts.getOrElse(Keys.ENCODING, FixedWidthConstants.DEFAULT_ENCODING)
  private val charset: Charset = Charset.forName(encoding)

  private val paddingChar: Char = opts.get(Keys.PADDING_CHAR.toLowerCase)
    .map(_.charAt(0))
    .getOrElse(FixedWidthConstants.DEFAULT_PADDING_CHAR)
  private val alignment: String = opts.getOrElse(Keys.ALIGNMENT.toLowerCase, FixedWidthConstants.DEFAULT_ALIGNMENT)
  private val lineEnding: Option[String] = opts.get(Keys.LINE_ENDING.toLowerCase)

  private val dateFormat: String = opts.getOrElse(Keys.DATE_FORMAT.toLowerCase, "yyyy-MM-dd")
  private val timestampFormat: String = opts.getOrElse(Keys.TIMESTAMP_FORMAT.toLowerCase, "yyyy-MM-dd HH:mm:ss")
  private val timeZone: ZoneId = ZoneId.of(opts.getOrElse(Keys.TIME_ZONE.toLowerCase, "UTC"))
  private val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
  private val tsFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(timestampFormat)

  // Create output file path unique per partition/task
  private val outputFilePath: Path = new Path(outputPath, f"part-$partitionId%05d-$taskId.txt")

  // Lazily initialize writer on first write
  private lazy val (fs, writer) = {
    val hadoopConf = SparkContext.getOrCreate().hadoopConfiguration
    val fsys = outputFilePath.getFileSystem(hadoopConf)
    val outputStream = fsys.create(outputFilePath, true)
    val w = new BufferedWriter(new OutputStreamWriter(outputStream, charset))
    (fsys, w)
  }

  private var rowsWritten: Long = 0

  /**
   * Parse field widths from options.
   * Priority: field_simple > field_lengths > schema_metadata_widths > schema metadata
   */
  private def parseFieldWidths(): Array[Int] = {
    opts.get(Keys.FIELD_SIMPLE.toLowerCase) match {
      case Some(simple) =>
        simple.split(",").map(_.trim.toInt)
      case None =>
        opts.get(Keys.FIELD_LENGTHS.toLowerCase) match {
          case Some(lengths) =>
            lengths.split(",").map { spec =>
              val parts = spec.trim.split(":")
              parts(1).toInt - parts(0).toInt  // end - start = width
            }
          case None =>
            opts.get(Keys.SCHEMA_METADATA_WIDTHS.toLowerCase) match {
              case Some(widths) =>
                widths.split(",").map(_.trim.toInt)
              case None =>
                // Try extracting from schema metadata
                val widths = schema.fields.flatMap { field =>
                  if (field.metadata.contains("width")) {
                    Some(field.metadata.getLong("width").toInt)
                  } else None
                }
                if (widths.length == schema.fields.length) {
                  widths
                } else {
                  throw new IllegalArgumentException(
                    "Either 'field_lengths', 'field_simple', 'schema_metadata_widths' option is required for writing, " +
                    "or all schema fields must have 'width' metadata")
                }
            }
        }
    }
  }

  /**
   * Format a value to a fixed-width string.
   * Respects paddingChar and alignment options.
   * Truncates if value exceeds field width.
   */
  private def formatValue(value: Any, width: Int, dataType: DataType): String = {
    val strValue = if (value == null) "" else value.toString
    if (strValue.length >= width) {
      strValue.substring(0, width)
    } else {
      val pad = paddingChar.toString * (width - strValue.length)
      alignment match {
        case "right" => pad + strValue
        case _       => strValue + pad
      }
    }
  }

  override def write(record: InternalRow): Unit = {
    val sb = new StringBuilder

    for (i <- 0 until Math.min(schema.fields.length, fieldWidths.length)) {
      val field = schema.fields(i)
      val value = if (record.isNullAt(i)) null else {
        field.dataType match {
          case StringType    => record.getString(i)
          case IntegerType   => record.getInt(i)
          case LongType      => record.getLong(i)
          case DoubleType    => record.getDouble(i)
          case FloatType     => record.getFloat(i)
          case BooleanType   => record.getBoolean(i)
          case d: DecimalType =>
            record.getDecimal(i, d.precision, d.scale).toJavaBigDecimal.toPlainString
          case DateType =>
            val days = record.getInt(i)
            LocalDate.ofEpochDay(days).format(dateFormatter)
          case TimestampType =>
            val micros = record.getLong(i)
            val instant = Instant.ofEpochSecond(micros / 1000000L, (micros % 1000000L) * 1000L)
            instant.atZone(timeZone).format(tsFormatter)
          case _ => record.get(i, field.dataType)
        }
      }
      sb.append(formatValue(value, fieldWidths(i), field.dataType))
    }

    writer.write(sb.toString())
    lineEnding match {
      case Some(le) => writer.write(le)
      case None     => writer.newLine()
    }
    rowsWritten += 1
  }

  override def commit(): WriterCommitMessage = {
    writer.flush()
    writer.close()
    FixedWidthWriterCommitMessage(partitionId, outputFilePath.toString, rowsWritten)
  }

  override def abort(): Unit = {
    try {
      writer.close()
    } catch {
      case _: Exception => // Ignore close errors on abort
    }
    try {
      fs.delete(outputFilePath, false)
    } catch {
      case _: Exception => // Ignore delete errors on abort
    }
  }

  override def close(): Unit = {
    try {
      writer.close()
    } catch {
      case _: Exception => // Ignore
    }
  }
}

/**
 * Commit message containing metadata about written partition.
 */
case class FixedWidthWriterCommitMessage(
  partitionId: Int,
  filePath: String,
  rowsWritten: Long
) extends WriterCommitMessage
