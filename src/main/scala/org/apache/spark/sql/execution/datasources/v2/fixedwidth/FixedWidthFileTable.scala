// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.execution.datasources.v2.fixedwidth

import java.util

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.alexandertimmer.fixedwidth.{FWUtils, FixedWidthConstants, FixedWidthWriteBuilder}

/**
 * [[FileTable]]-based table for the fixed-width source.
 *
 * File discovery (globs, directories, explicit path lists, hidden-file
 * filtering, Hive-style partition inference) is inherited from [[FileTable]]'s
 * `PartitioningAwareFileIndex`. Schema rules and the write path are the
 * pre-migration ones, unchanged:
 *  - inferred schema = `FWUtils.inferBaseSchema` + special-column appending
 *    (user-supplied schemas get special columns resolved in the provider);
 *  - writes go through the existing [[FixedWidthWriteBuilder]].
 *
 * @since 0.2.0
 */
case class FixedWidthFileTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema)
    with SupportsMetadataColumns {

  override def newScanBuilder(options: CaseInsensitiveStringMap): FixedWidthFileScanBuilder =
    FixedWidthFileScanBuilder(sparkSession, fileIndex, schema, dataSchema, mergedOptions(options))

  /**
   * Whether the table location resolves to an existing path. `FileTable`'s
   * `fileIndex` eagerly validates that all paths exist — correct for reads,
   * but this source (unlike the built-ins, whose writes Spark routes to V1)
   * also serves `df.write.save(newPath)` through this table, where the output
   * location does not exist yet. Schema resolution below therefore only
   * consults the file index when the location exists; reads of truly missing
   * paths still fail loudly when the scan accesses `fileIndex`.
   */
  private lazy val fileIndexResolves: Boolean = scala.util.Try(fileIndex).isSuccess

  private def schemaFromOptions: StructType =
    FWUtils.appendSpecialColumns(FWUtils.inferBaseSchema(options), options)

  // NOTE on the re-implemented bodies below: they mirror FileTable's logic but
  // deliberately avoid Spark-internal helper objects (SchemaUtils,
  // PartitioningUtils, QueryCompilationErrors) whose method signatures differ
  // between OSS Spark and Databricks runtimes and cause NoSuchMethodError when
  // this jar (compiled against OSS) runs on DBR. The trivial logic is inlined.

  /** `PartitioningUtils.getColName` inlined (internal-API-safe). */
  private def colName(field: StructField, caseSensitive: Boolean): String =
    if (caseSensitive) field.name else field.name.toLowerCase(java.util.Locale.ROOT)

  /** `SchemaUtils.checkSchemaColumnNameDuplication` inlined (internal-API-safe). */
  private def checkNoDuplicateColumns(schema: StructType, caseSensitive: Boolean): Unit = {
    val duplicates = schema.fields.map(colName(_, caseSensitive))
      .groupBy(identity).collect { case (name, group) if group.length > 1 => name }
    require(duplicates.isEmpty,
      s"Found duplicate column(s) in the schema: ${duplicates.mkString(", ")}")
  }

  // Guarded re-implementation of FileTable.dataSchema (super access to a lazy
  // val is not possible): identical behavior when the location exists.
  override lazy val dataSchema: StructType = {
    if (fileIndexResolves) {
      val schema = userSpecifiedSchema.map { schema =>
        val partitionSchema = fileIndex.partitionSchema
        val resolver = sparkSession.sessionState.conf.resolver
        StructType(schema.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name))))
      }.orElse {
        inferSchema(fileIndex.allFiles())
      }.getOrElse {
        throw new IllegalArgumentException(
          s"Unable to infer schema for $formatName. It must be specified manually.")
      }
      schema.asNullable
    } else {
      userSpecifiedSchema.getOrElse(schemaFromOptions).asNullable
    }
  }

  // Guarded re-implementation of FileTable.schema: identical when the location
  // exists; a not-yet-existing location has no partition directories to merge.
  override lazy val schema: StructType = {
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    checkNoDuplicateColumns(dataSchema, caseSensitive)
    if (fileIndexResolves) {
      val partitionSchema = fileIndex.partitionSchema
      checkNoDuplicateColumns(partitionSchema, caseSensitive)
      val partitionNameSet: Set[String] =
        partitionSchema.fields.map(colName(_, caseSensitive)).toSet
      val fields = dataSchema.fields.filterNot { field =>
        partitionNameSet.contains(colName(field, caseSensitive))
      } ++ partitionSchema.fields
      StructType(fields)
    } else {
      dataSchema
    }
  }

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(FWUtils.appendSpecialColumns(FWUtils.inferBaseSchema(options), options))

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    // The existing write path resolves its output location from the `path`
    // option; re-add the path this provider stripped into `paths`.
    val writeOptions = new util.HashMap[String, String](options.asCaseSensitiveMap())
    paths.headOption.foreach(writeOptions.put(FixedWidthConstants.OptionKeys.PATH, _))
    FixedWidthWriteBuilder(schema, new CaseInsensitiveStringMap(writeOptions), info)
  }

  // The pre-migration reader/writer accept any schema the user supplies
  // (type support is enforced per-value at parse time), so keep accepting all.
  override def supportsDataType(dataType: DataType): Boolean = true

  override def formatName: String = "FixedWidth"

  /**
   * Declares the `_metadata` column (same shape as Spark's file-source
   * metadata struct), which the analyzer resolves for queries referencing
   * `_metadata` / `_metadata.file_path`. Values are synthesized per file in
   * [[FixedWidthFilePartitionReaderFactory]].
   */
  override def metadataColumns(): Array[MetadataColumn] =
    Array(new MetadataColumn {
      override def name: String = FixedWidthFileTable.METADATA_COL_NAME
      override def dataType: DataType = FixedWidthFileTable.METADATA_STRUCT
      override def isNullable: Boolean = false
      override def comment: String = "Per-row source file metadata"
    })

  // Same capability set as the pre-migration FixedWidthTable (FileTable's own
  // set lacks the truncate/overwrite capabilities the existing write tests use).
  override def capabilities: util.Set[TableCapability] =
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA,
      TableCapability.TRUNCATE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC
    ).asJava
}

object FixedWidthFileTable {

  val METADATA_COL_NAME = "_metadata"

  /** Same field names, types and order as Spark's `FileFormat.BASE_METADATA_FIELDS`. */
  val METADATA_STRUCT: StructType = StructType(Seq(
    StructField("file_path", StringType, nullable = false),
    StructField("file_name", StringType, nullable = false),
    StructField("file_size", LongType, nullable = false),
    StructField("file_block_start", LongType, nullable = false),
    StructField("file_block_length", LongType, nullable = false),
    StructField("file_modification_time", TimestampType, nullable = false)
  ))
}
