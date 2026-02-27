// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.hadoop.fs.Path

import FixedWidthConstants._
import FixedWidthConstants.{OptionKeys => Keys}

/**
 * Builder for creating `FixedWidthScan` instances.
 *
 * This class implements the `ScanBuilder` interface to construct
 * scan objects for reading fixed-width files.
 *
 * @param schema resolved schema for the scan
 * @param options data source options
 * @since 0.1.0
 */
case class FixedWidthScanBuilder(schema: org.apache.spark.sql.types.StructType,
                                 options: org.apache.spark.sql.util.CaseInsensitiveStringMap)
  extends ScanBuilder {

  /** @return configured scan for fixed-width files */
  override def build(): Scan =
    FixedWidthScan(schema, options)
}

/**
 * Scan implementation for reading fixed-width files.
 *
 * This class handles partition planning and reader factory creation for
 * batch reading of fixed-width formatted text files.
 *
 * ==Partition Planning==
 * Files are partitioned based on:
 *  - `numPartitions` option: explicit partition count (single file only)
 *  - `maxPartitionBytes` option: maximum bytes per partition (default 128MB)
 *  - Compressed files are never split (single partition per file)
 *  - Multiple files: one logical partition per file
 *
 * ==Glob Pattern Support==
 * The `path` option supports Hadoop glob patterns including wildcards,
 * character classes, and brace expansions for matching multiple files.
 *
 * @param schema resolved schema including special columns
 * @param options data source options
 * @since 0.1.0
 */
case class FixedWidthScan(schema: org.apache.spark.sql.types.StructType,
                          options: org.apache.spark.sql.util.CaseInsensitiveStringMap)
  extends Scan
    with Batch {

  /** @return schema used for reading */
  override def readSchema() = schema

  /** @return this instance as a batch scan */
  override def toBatch: Batch = this

  /**
   * Plans input partitions for reading fixed-width files.
   *
   * This method:
   *  1. Resolves glob patterns to find matching files
   *  2. Expands directories to list contained files
   *  3. Filters hidden files (prefixed with `_` or `.`)
   *  4. Calculates partition boundaries based on file size
   *  5. Handles compressed files as single partitions
   *
   * @return array of input partitions for parallel reading
   * @throws IllegalArgumentException if `path` option is missing
   * @throws java.io.FileNotFoundException if no files match the pattern
   */
  override def planInputPartitions(): Array[InputPartition] = {
    val pathPattern = Option(options.get(Keys.PATH))
      .getOrElse(throw new IllegalArgumentException("Option 'path' is required"))

    val spark = SparkSession.active
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val path = new Path(pathPattern)
    val fs = path.getFileSystem(hadoopConf)

    // Expand glob patterns to get all matching files
    // If path is a directory, list its contents; otherwise use globStatus
    val fileStatuses = Option(fs.globStatus(path)) match {
      case Some(statuses) if statuses.nonEmpty =>
        // Expand directories to their contents (recursively list files)
        statuses.flatMap { status =>
          if (status.isDirectory) {
            // List files in directory, filter out hidden files (like _SUCCESS, .crc)
            fs.listStatus(status.getPath)
              .filter(_.isFile)
              .filterNot(_.getPath.getName.startsWith("_"))
              .filterNot(_.getPath.getName.startsWith("."))
          } else {
            Array(status)
          }
        }
      case _ => throw new java.io.FileNotFoundException(
        s"No files found matching pattern: $pathPattern"
      )
    }

    // Ensure we found at least one file
    if (fileStatuses.isEmpty) {
      throw new java.io.FileNotFoundException(
        s"No files found matching pattern: $pathPattern"
      )
    }

    // Check for compression codecs - compressed files cannot be split
    val codecFactory = new org.apache.hadoop.io.compress.CompressionCodecFactory(hadoopConf)

    // Check for explicit numPartitions option first (with validation)
    val numPartitionsOpt = FWUtils.parseNumPartitions(options)

    // Default maxPartitionBytes is 128MB (like Spark CSV) - with validation
    val maxPartitionBytes = FWUtils.parseMaxPartitionBytes(options)

    // Create partitions for each file (with potential splitting for large files)
    fileStatuses.flatMap { fileStatus =>
      val filePath = fileStatus.getPath.toString
      val fileLength = fileStatus.getLen
      val isCompressed = Option(codecFactory.getCodec(fileStatus.getPath)).isDefined

      // Compressed files cannot be split - always single partition
      if (isCompressed) {
        Seq(FixedWidthPartition(filePath, start = 0, length = fileLength, isFirstSplit = true))
      } else {
        // Calculate number of partitions for this file
        val numPartitions = numPartitionsOpt match {
          case Some(n) if fileStatuses.length == 1 => n  // User-specified for single file
          case Some(_) => 1  // Multiple files: one partition per file
          case None =>
            // Calculate based on file size and maxPartitionBytes
            Math.max(1, Math.ceil(fileLength.toDouble / maxPartitionBytes).toInt)
        }

        if (numPartitions == 1) {
          // Single partition for this file
          Seq(FixedWidthPartition(filePath, start = 0, length = fileLength, isFirstSplit = true))
        } else {
          // Multiple partitions - calculate byte ranges
          val splitSize = fileLength / numPartitions

          (0 until numPartitions).map { i =>
            val start = i * splitSize
            val length = if (i == numPartitions - 1) fileLength - start else splitSize
            FixedWidthPartition(
              path = filePath,
              start = start,
              length = length,
              isFirstSplit = (i == 0)
            )
          }
        }
      }
    }
  }

  /**
   * Creates a factory for partition readers.
   *
   * This method resolves all configuration options and creates a factory
   * that will construct partition readers with consistent settings:
   *  - Field positions (from `field_lengths`, `field_simple`, or schema metadata)
   *  - Trim options (`trimValues`, `ignoreLeadingWhiteSpace`, `ignoreTrailingWhiteSpace`)
   *  - Skip lines (`header` takes precedence over `skip_lines`)
   *  - Parse mode (PERMISSIVE, DROPMALFORMED, FAILFAST)
   *  - Encoding, null value, date/time formats
   *  - Error handling columns (`rescuedDataColumn`, `columnNameOfCorruptRecord`)
   *
   * @return factory for creating partition readers
   */
  override def createReaderFactory(): PartitionReaderFactory = {
    // Resolve trim options:
    // - trimValues=true (default) enables both leading and trailing trim
    // - ignoreLeadingWhiteSpace/ignoreTrailingWhiteSpace override trimValues if set
    val trimValuesOpt = Option(options.get("trimValues")).map(_.toBoolean)
    val defaultTrim = trimValuesOpt.getOrElse(true)

    val ignoreLeading = Option(options.get(Keys.IGNORE_LEADING_WHITE_SPACE))
      .map(_.toBoolean)
      .getOrElse(defaultTrim)

    val ignoreTrailing = Option(options.get(Keys.IGNORE_TRAILING_WHITE_SPACE))
      .map(_.toBoolean)
      .getOrElse(defaultTrim)

    // Resolve field positions: field_simple > field_lengths > schema metadata widths
    val fieldPositions = FWUtils.parsePositionsWithSchema(options, schema)
    // Convert back to field_lengths format for PartitionReader
    val fieldLengthsStr = fieldPositions.map { case (s, e) => s"$s:$e" }.mkString(",")

    // Resolve skip_lines: header=true takes precedence over skip_lines
    // header=true → skip 1 line, header=false → skip 0, not set → use skip_lines (with validation)
    val skipLines = Option(options.get("header")) match {
      case Some(h) if h.equalsIgnoreCase("true") => 1
      case Some(_) => 0  // header=false explicitly set
      case None => FWUtils.parseSkipLines(options)
    }

    FixedWidthPartitionReaderFactory(
      schema = schema,
      fieldLengths = fieldLengthsStr,
      mode = Option(options.get(Keys.MODE)).map(_.toUpperCase).getOrElse(DEFAULT_MODE),
      skipLines = skipLines,
      encoding = Option(options.get(Keys.ENCODING)).getOrElse(DEFAULT_ENCODING),
      rescuedDataColumn = Option(options.get(Keys.RESCUED_DATA_COLUMN)),
      columnNameOfCorruptRecord = Option(options.get(Keys.COLUMN_NAME_OF_CORRUPT_RECORD)),
      ignoreLeading,
      ignoreTrailing,
      nullValue = Option(options.get(Keys.NULL_VALUE)),
      dateFormat = Option(options.get(Keys.DATE_FORMAT)),
      timestampFormat = Option(options.get(Keys.TIMESTAMP_FORMAT)),
      timeZone = Option(options.get(Keys.TIME_ZONE)),
      comment = Option(options.get(Keys.COMMENT)).map(_.charAt(0))
    )
  }
}
