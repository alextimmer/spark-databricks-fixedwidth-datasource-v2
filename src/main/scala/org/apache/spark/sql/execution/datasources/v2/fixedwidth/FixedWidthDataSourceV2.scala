// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.execution.datasources.v2.fixedwidth

import java.util

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.alexandertimmer.fixedwidth.{FWUtils, FixedWidthConstants}

/**
 * DataSource V2 provider for the fixed-width source, built on Spark's native
 * FileScan machinery via [[FixedWidthFileTable]].
 *
 * ==Why this implements [[TableProvider]] directly and NOT `FileDataSourceV2`==
 * Spark's `DataFrameWriter` unconditionally excludes `FileDataSourceV2` sources
 * from the V2 write path (SPARK-28396: `lookupV2Provider()` returns `None` for
 * them) and routes writes through the V1 `fallbackFileFormat` instead — which
 * for this source is a throwing shim. Implementing `TableProvider` directly
 * keeps writes on the existing, battle-tested `FixedWidthWriteBuilder` V2 path,
 * while reads still gain the full FileScan machinery (explicit path lists,
 * cross-file bin-packing, file provenance) through [[FixedWidthFileTable]].
 *
 * The `paths`/`path` option handling below mirrors `FileDataSourceV2.getPaths`
 * so that `spark.read.format(...).load(f1, f2, ...)` (which Spark encodes as a
 * JSON `paths` option) behaves exactly like the built-in file sources.
 *
 * @since 0.2.0
 */
class FixedWidthDataSourceV2 extends TableProvider with DataSourceRegister {

  lazy val sparkSession: SparkSession = SparkSession.active

  override def shortName(): String = FixedWidthConstants.FORMAT_SHORT_NAME

  override def supportsExternalMetadata(): Boolean = true

  /** Mirrors `FileDataSourceV2.getPaths`: JSON-encoded `paths` list plus singular `path`. */
  private def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(map.get("paths")).map { pathStr =>
      FixedWidthDataSourceV2.readPathsToSeq(pathStr)
    }.getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }

  /** Mirrors `FileDataSourceV2.getOptionsWithoutPaths`. */
  private def getOptionsWithoutPaths(map: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val withoutPath = map.asCaseSensitiveMap().asScala.filter { case (k, _) =>
      !k.equalsIgnoreCase("path") && !k.equalsIgnoreCase("paths")
    }
    new CaseInsensitiveStringMap(withoutPath.toMap.asJava)
  }

  /** Mirrors `FileDataSourceV2.getTableName` (qualified, redacted path list). */
  private def getTableName(map: CaseInsensitiveStringMap, paths: Seq[String]): String = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(
      map.asCaseSensitiveMap().asScala.toMap)
    val name = shortName() + " " + paths.map(qualifiedPathName(_, hadoopConf)).mkString(",")
    redactSensitive(name)
  }

  /**
   * Applies `spark.sql.redaction.string.regex` to the display name. Inlined
   * instead of calling `org.apache.spark.util.Utils.redact`: that internal
   * helper's signature differs between OSS Spark and Databricks runtimes
   * (NoSuchMethodError on DBR 17.x). The name is display-only, so any linkage
   * problem falls back to the unredacted name instead of failing the read.
   */
  private def redactSensitive(text: String): String = {
    try {
      sparkSession.sessionState.conf.stringRedactionPattern match {
        case Some(regex) => regex.replaceAllIn(text, "*********(redacted)")
        case None => text
      }
    } catch {
      case _: LinkageError => text
    }
  }

  private def qualifiedPathName(path: String, hadoopConf: Configuration): String = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
  }

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    FixedWidthFileTable(tableName, sparkSession, optionsWithoutPaths, paths,
      userSpecifiedSchema = None, classOf[FixedWidthFileFormat])
  }

  def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    // Preserve the pre-migration special-column contract for user-supplied schemas:
    // `_corrupt_record` is honored only if already present (never auto-appended),
    // the rescued data column IS auto-appended when its option is set.
    val resolvedSchema = FWUtils.appendSpecialColumns(schema, options)
    FixedWidthFileTable(tableName, sparkSession, optionsWithoutPaths, paths,
      userSpecifiedSchema = Some(resolvedSchema), classOf[FixedWidthFileFormat])
  }

  // Mirrors FileDataSourceV2's TableProvider stubs: the table created during
  // schema inference is reused by the subsequent getTable call of the same load.
  private var t: Table = null

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    if (t == null) t = getTable(options)
    t.columns.asSchema
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    Array.empty
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    // If the table is already loaded during schema inference, return it directly.
    if (t != null) {
      t
    } else {
      getTable(new CaseInsensitiveStringMap(properties), schema)
    }
  }
}

object FixedWidthDataSourceV2 {
  // Same JSON decoding Spark uses for the DataFrameReader.load(paths: _*) vararg list
  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private[fixedwidth] def readPathsToSeq(paths: String): Seq[String] =
    objectMapper.readValue(paths, classOf[Seq[String]])
}
