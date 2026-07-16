// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.execution.datasources.v2.fixedwidth

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import com.alexandertimmer.fixedwidth.{FWUtils, FixedWidthConstants}
import com.alexandertimmer.fixedwidth.FixedWidthConstants.{OptionKeys => Keys}

/**
 * [[TextBasedFileScan]]-based scan for fixed-width files.
 *
 * Partition planning (splitting, cross-file bin-packing, compression handling)
 * is inherited from the native FileScan machinery. `createReaderFactory()`
 * ports the pre-migration Driver-side option resolution unchanged in substance
 * (same options, same defaults, same `SerializableConfiguration` approach,
 * same rescued-data file-path conf resolution).
 *
 * @since 0.2.0
 */
case class FixedWidthFileScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    readMetadataSchema: StructType = new StructType(),
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends TextBasedFileScan(sparkSession, options) {

  /** Appends the `_metadata` struct when the query requested it (see the scan builder). */
  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields ++ readMetadataSchema.fields)

  /**
   * Partition planning override preserving the pre-migration datasource options
   * (`numPartitions`, `maxPartitionBytes`) on top of the native machinery:
   *  - neither option set → pure native planning (session confs, open-cost
   *    bin-packing across files);
   *  - `maxPartitionBytes` → takes precedence over the session-conf-derived
   *    split size;
   *  - `numPartitions` + single splittable file → exact split count
   *    (pre-migration semantics);
   *  - `numPartitions` + multiple files → global target: split size
   *    `ceil(totalBytes / n)` fed into native bin-packing (the option no
   *    longer disables splitting);
   *  - compressed files are never split, regardless.
   * Hive-style partitioned directory layouts use native planning — the
   * datasource options above are single-directory/file-list features.
   */
  override protected def partitions: Seq[FilePartition] = {
    val numPartitionsOpt = FWUtils.parseNumPartitions(options)
    val maxPartitionBytesSet = Option(options.get(Keys.MAX_PARTITION_BYTES)).isDefined

    if ((numPartitionsOpt.isEmpty && !maxPartitionBytesSet) || fileIndex.partitionSchema.nonEmpty) {
      return super.partitions
    }

    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)

    def packedPartitions(maxSplitBytes: Long): Seq[FilePartition] = {
      val splitFiles = selectedPartitions.flatMap { partition =>
        partition.files.flatMap { file =>
          PartitionedFileUtil.splitFiles(
            file = file,
            filePath = file.getPath,
            isSplitable = isSplitable(file.getPath),
            maxSplitBytes = maxSplitBytes,
            partitionValues = partition.values)
        }
      }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
      FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
    }

    numPartitionsOpt match {
      case Some(n) =>
        val allFiles = selectedPartitions.flatMap(_.files)
        if (allFiles.length == 1 && isSplitable(allFiles.head.getPath)) {
          // Single splittable file: exact partition count (pre-migration math).
          val file = allFiles.head
          val partitionValues = selectedPartitions.head.values
          val fileLength = file.getLen
          val splitSize = fileLength / n
          val splits = (0 until n).map { i =>
            val start = i.toLong * splitSize
            val length = if (i == n - 1) fileLength - start else splitSize
            PartitionedFileUtil.getPartitionedFile(
              file, file.getPath, partitionValues, start, length)
          }
          splits.zipWithIndex.map { case (split, i) => FilePartition(i, Array(split)) }
        } else if (allFiles.length <= 1) {
          // Single non-splittable (compressed) file or empty listing: native planning.
          super.partitions
        } else {
          // Multiple files: numPartitions is a global target for bin-packing.
          val totalBytes = allFiles.map(_.getLen).sum
          val targetSplitBytes = math.max(1L, (totalBytes + n - 1) / n)
          packedPartitions(targetSplitBytes)
        }
      case None =>
        packedPartitions(FWUtils.parseMaxPartitionBytes(options))
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Resolve Hadoop config and Spark SQL config on the Driver (not available on Executors)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val serializableHadoopConf = new SerializableConfiguration(
      sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap))
    val includeFilePathInRescuedData: Boolean = {
      val confKey = FixedWidthConstants.SparkConfKeys.RESCUED_DATA_FILE_PATH_ENABLED
      scala.util.Try(sparkSession.conf.get(confKey)).toOption.forall(_ != "false")
    }

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
    val fieldPositions = FWUtils.parsePositionsWithSchema(options, readDataSchema)
    // Convert back to field_lengths format for PartitionReader
    val fieldLengthsStr = fieldPositions.map { case (s, e) => s"$s:$e" }.mkString(",")

    // Resolve skip_lines: header=true takes precedence over skip_lines
    // header=true → skip 1 line, header=false → skip 0, not set → use skip_lines (with validation)
    val skipLines = Option(options.get("header")) match {
      case Some(h) if h.equalsIgnoreCase("true") => 1
      case Some(_) => 0 // header=false explicitly set
      case None => FWUtils.parseSkipLines(options)
    }

    FixedWidthFilePartitionReaderFactory(
      schema = readDataSchema,
      fieldLengths = fieldLengthsStr,
      mode = Option(options.get(Keys.MODE)).map(_.toUpperCase).getOrElse(
        FixedWidthConstants.DEFAULT_MODE),
      skipLines = skipLines,
      encoding = Option(options.get(Keys.ENCODING)).getOrElse(
        FixedWidthConstants.DEFAULT_ENCODING),
      rescuedDataColumn = Option(options.get(Keys.RESCUED_DATA_COLUMN)),
      columnNameOfCorruptRecord = Option(options.get(Keys.COLUMN_NAME_OF_CORRUPT_RECORD)),
      ignoreLeadingWhiteSpace = ignoreLeading,
      ignoreTrailingWhiteSpace = ignoreTrailing,
      nullValue = Some(Option(options.get(Keys.NULL_VALUE)).getOrElse(
        FixedWidthConstants.DEFAULT_NULL_VALUE)),
      dateFormat = Option(options.get(Keys.DATE_FORMAT)),
      timestampFormat = Option(options.get(Keys.TIMESTAMP_FORMAT)),
      timeZone = Option(options.get(Keys.TIME_ZONE)),
      comment = Option(options.get(Keys.COMMENT)).map(_.charAt(0)),
      hadoopConf = serializableHadoopConf,
      includeFilePathInRescuedData = includeFilePathInRescuedData,
      emptyValue = Option(options.get(Keys.EMPTY_VALUE)),
      nanValue = Option(options.get(Keys.NAN_VALUE)).getOrElse(
        FixedWidthConstants.DEFAULT_NAN_VALUE),
      positiveInf = Option(options.get(Keys.POSITIVE_INF)).getOrElse(
        FixedWidthConstants.DEFAULT_POSITIVE_INF),
      negativeInf = Option(options.get(Keys.NEGATIVE_INF)).getOrElse(
        FixedWidthConstants.DEFAULT_NEGATIVE_INF),
      partitionSchema = readPartitionSchema,
      metadataSchema = readMetadataSchema,
      options = new FileSourceOptions(CaseInsensitiveMap(caseSensitiveMap))
    )
  }

  override def equals(obj: Any): Boolean = obj match {
    case f: FixedWidthFileScan =>
      super.equals(f) && dataSchema == f.dataSchema && options == f.options
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}
