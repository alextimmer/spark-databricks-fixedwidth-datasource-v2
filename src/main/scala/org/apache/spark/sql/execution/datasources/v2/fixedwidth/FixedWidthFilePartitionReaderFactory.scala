// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.execution.datasources.v2.fixedwidth

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import com.alexandertimmer.fixedwidth.FixedWidthPartitionReader

/**
 * [[FilePartitionReaderFactory]] for fixed-width files.
 *
 * Bridges Spark's native [[PartitionedFile]] splits to the existing
 * [[FixedWidthPartitionReader]] (whose per-line parsing logic is unchanged):
 * path/start/length map directly, and `isFirstSplit` is `start == 0` — the
 * same first-split semantics the pre-migration planner produced. Reading rows
 * through this factory also populates Spark's input-file metadata, which is
 * what makes `input_file_name()` work.
 *
 * @since 0.2.0
 */
case class FixedWidthFilePartitionReaderFactory(
    schema: StructType,
    fieldLengths: String,
    mode: String,
    skipLines: Int,
    encoding: String,
    rescuedDataColumn: Option[String],
    columnNameOfCorruptRecord: Option[String],
    ignoreLeadingWhiteSpace: Boolean,
    ignoreTrailingWhiteSpace: Boolean,
    nullValue: Option[String],
    dateFormat: Option[String],
    timestampFormat: Option[String],
    timeZone: Option[String],
    comment: Option[Char],
    hadoopConf: SerializableConfiguration,
    includeFilePathInRescuedData: Boolean,
    emptyValue: Option[String],
    nanValue: String,
    positiveInf: String,
    negativeInf: String,
    partitionSchema: StructType,
    metadataSchema: StructType,
    options: FileSourceOptions
) extends FilePartitionReaderFactory {

  /**
   * Builds the `_metadata` struct value for one file split. Field order,
   * types and value semantics mirror Spark's `FileFormat.BASE_METADATA_EXTRACTORS`.
   */
  private def metadataStruct(file: PartitionedFile): InternalRow = {
    InternalRow(
      UTF8String.fromString(new Path(file.filePath.toPath.toString).toUri.toString),
      UTF8String.fromString(file.filePath.toUri.getRawPath.split("/").lastOption.getOrElse("")),
      file.fileSize,
      file.start,
      file.length,
      file.modificationTime * 1000L // millis → TimestampType micros
    )
  }

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val reader = new FixedWidthPartitionReader(
      pathStr = file.toPath.toString,
      startByte = file.start,
      lengthBytes = file.length,
      isFirstSplit = file.start == 0,
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
      comment = comment,
      hadoopConf = hadoopConf.value,
      includeFilePathInRescuedData = includeFilePathInRescuedData,
      emptyValue = emptyValue,
      nanValue = nanValue,
      positiveInf = positiveInf,
      negativeInf = negativeInf
    )
    // Appends Hive-style partition-directory values (no-op for flat layouts)
    val withPartitionValues =
      new PartitionReaderWithPartitionValues(reader, schema, partitionSchema, file.partitionValues)

    if (metadataSchema.isEmpty) {
      withPartitionValues
    } else {
      // Appends the requested `_metadata` struct as the trailing column
      new PartitionReaderWithPartitionValues(
        withPartitionValues,
        StructType(schema.fields ++ partitionSchema.fields),
        metadataSchema,
        InternalRow(metadataStruct(file)))
    }
  }
}
