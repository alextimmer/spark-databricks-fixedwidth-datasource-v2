// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.fixedwidth.{FixedWidthDataSourceV2, FixedWidthFileFormat, FixedWidthFileTable}
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => JPath, Paths}
import scala.jdk.CollectionConverters._

/**
 * Phase 0 test suite for the FileScan migration
 * (spec: docs/superpowers/specs/2026-07-16-filescan-migration-design.md).
 *
 * Two groups:
 *
 *  - "characterization:" tests pin CURRENT behavior that must survive the
 *    migration unchanged. They pass against the pre-migration code and must
 *    keep passing at every phase gate.
 *
 *  - "gap:" tests describe the four headline gaps the migration closes
 *    (explicit path lists, file provenance, cross-file bin-packing,
 *    numPartitions multi-file footgun). They FAIL against the pre-migration
 *    code by design and must pass from Phase 2 (read path cut-over) onward.
 */
class FileScanMigrationSpec extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("FileScanMigrationTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val testDataPath: String = "data/test_data"

  private val nameIdSchema = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("id", IntegerType, nullable = true)
  ))

  private def readFixedWidth(fieldLengths: String,
                             schema: StructType,
                             extraOptions: Map[String, String] = Map.empty)(paths: String*): DataFrame = {
    val reader = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", fieldLengths)
      .schema(schema)
    extraOptions.foreach { case (k, v) => reader.option(k, v) }
    reader.load(paths: _*)
  }

  private def withTempDir(testCode: JPath => Unit): Unit = {
    val dir = Files.createTempDirectory("fw-filescan-spec")
    try {
      testCode(dir)
    } finally {
      val stream = Files.walk(dir)
      try {
        stream.sorted(java.util.Comparator.reverseOrder[JPath]())
          .forEach((p: JPath) => Files.deleteIfExists(p))
      } finally {
        stream.close()
      }
    }
  }

  private def writeFile(dir: JPath, name: String, content: String): JPath =
    Files.write(dir.resolve(name), content.getBytes(StandardCharsets.UTF_8))

  private def withSqlConf(pairs: (String, String)*)(testCode: => Unit): Unit = {
    val previous = pairs.map { case (k, _) =>
      // Unset optional confs may surface as an exception OR as a null value
      k -> scala.util.Try(Option(spark.conf.get(k))).toOption.flatten
    }
    pairs.foreach { case (k, v) => spark.conf.set(k, v) }
    try {
      testCode
    } finally {
      previous.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None)    => spark.conf.unset(k)
      }
    }
  }

  // ===========================================================================
  // 0a. Characterization tests — pin current behavior (must ALWAYS pass)
  // ===========================================================================

  test("characterization: single-file read parses values and row count") {
    val rows = readFixedWidth("0:5,5:10", nameIdSchema)(s"$testDataPath/valid1.txt").collect()

    assert(rows.length == 3)
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(0).getAs[Int]("id") == 1)
    assert(rows(2).getAs[String]("name") == "Carol")
    assert(rows(2).getAs[Int]("id") == 3)
  }

  test("characterization: glob read returns all rows from all matched files") {
    val rows = readFixedWidth("0:10,10:13", nameIdSchema)(s"$testDataPath/multi_file_*.txt").collect()

    assert(rows.length == 8, s"Expected 8 rows across 3 files, got ${rows.length}")
    val ids = rows.map(_.getAs[Int]("id")).toSet
    assert(ids == (1 to 8).toSet, s"Expected ids 1..8, got $ids")
  }

  test("characterization: directory read skips hidden files (underscore and dot prefixes)") {
    withTempDir { dir =>
      writeFile(dir, "one.txt", "Alice00001\n")
      writeFile(dir, "two.txt", "Bob  00002\n")
      writeFile(dir, "_hidden.txt", "Evil 00099\n")
      writeFile(dir, ".hidden.txt", "Evil 00098\n")

      val rows = readFixedWidth("0:5,5:10", nameIdSchema)(dir.toString).collect()

      assert(rows.length == 2, s"Expected 2 rows (hidden files skipped), got ${rows.length}")
      assert(rows.map(_.getAs[String]("name")).toSet == Set("Alice", "Bob"))
    }
  }

  test("characterization: compressed file reads fully and is never split") {
    val df = readFixedWidth("0:5,5:10", nameIdSchema,
      Map("numPartitions" -> "4"))(s"$testDataPath/valid1.txt.gz")

    assert(df.rdd.getNumPartitions == 1,
      s"Compressed file must be a single partition, got ${df.rdd.getNumPartitions}")
    val rows = df.collect()
    assert(rows.length == 3)
    assert(rows(0).getAs[String]("name") == "Alice")
  }

  test("characterization: write round-trip preserves values") {
    withTempDir { dir =>
      val outputPath = dir.resolve("roundtrip_out").toString

      Seq(("Alice", 1), ("Bob", 2)).toDF("name", "id")
        .coalesce(1)
        .write
        .format("fixedwidth-custom-scala")
        .option("field_lengths", "0:10,10:15")
        .mode("overwrite")
        .save(outputPath)

      val rows = readFixedWidth("0:10,10:15", nameIdSchema)(outputPath)
        .collect()
        .sortBy(_.getAs[Int]("id"))

      assert(rows.length == 2)
      assert(rows(0).getAs[String]("name") == "Alice" && rows(0).getAs[Int]("id") == 1)
      assert(rows(1).getAs[String]("name") == "Bob" && rows(1).getAs[Int]("id") == 2)
    }
  }

  test("characterization: both special columns set — corrupt stays NULL, rescued carries JSON") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true),
      StructField("_rescued_data", StringType, nullable = true)
    ))

    val rows = readFixedWidth("0:5,5:10", schema, Map(
      "mode" -> "PERMISSIVE",
      "columnNameOfCorruptRecord" -> "_corrupt_record",
      "rescuedDataColumn" -> "_rescued_data"
    ))(s"$testDataPath/invalid1.txt").collect()

    assert(rows.length == 3)
    // Valid row: both special columns NULL
    assert(rows(0).isNullAt(rows(0).fieldIndex("_corrupt_record")))
    assert(rows(0).isNullAt(rows(0).fieldIndex("_rescued_data")))
    // Malformed row ("0000A" for IntegerType): corrupt NULL, rescued JSON
    assert(rows(2).isNullAt(rows(2).fieldIndex("_corrupt_record")),
      "_corrupt_record must stay NULL when both options are set (rescued takes precedence)")
    val rescuedJson = rows(2).getAs[String]("_rescued_data")
    assert(rescuedJson != null && rescuedJson.contains("\"id\"") && rescuedJson.contains("0000A"))
  }

  test("characterization: rescuedDataColumn.filePath.enabled conf controls _file_path in JSON") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    val confKey = "spark.databricks.sql.rescuedDataColumn.filePath.enabled"

    def rescuedJsonOfBadRow(): String = {
      val rows = readFixedWidth("0:5,5:10", schema, Map(
        "rescuedDataColumn" -> "_rescued_data"
      ))(s"$testDataPath/invalid1.txt").collect()
      rows(2).getAs[String]("_rescued_data")
    }

    withSqlConf(confKey -> "true") {
      assert(rescuedJsonOfBadRow().contains("_file_path"),
        "_file_path must be included when conf is true (default)")
    }
    withSqlConf(confKey -> "false") {
      assert(!rescuedJsonOfBadRow().contains("_file_path"),
        "_file_path must be omitted when conf is false")
    }
  }

  test("characterization: short lines accepted — missing trailing field becomes null") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    ))

    val rows = readFixedWidth("0:10,10:15", schema)(s"$testDataPath/short_line_test.txt").collect()

    assert(rows.length == 3)
    assert(rows(0).getAs[String]("value") == "Hello")
    assert(rows(1).getAs[String]("value") == "Hel")
    assert(rows(2).getAs[String]("name") == "Carol")
    assert(rows(2).isNullAt(rows(2).fieldIndex("value")),
      "Missing trailing field on a short line must be null (nullValue=\"\" default)")
  }

  test("characterization: forced split boundaries lose and duplicate no rows") {
    val df = readFixedWidth("0:7,7:12", nameIdSchema,
      Map("maxPartitionBytes" -> "500"))(s"$testDataPath/large_test.txt")

    assert(df.rdd.getNumPartitions >= 2,
      s"maxPartitionBytes=500 on a 1300-byte file must split, got ${df.rdd.getNumPartitions}")
    val ids = df.collect().map(_.getAs[Int]("id"))
    assert(ids.length == 100, s"Expected 100 rows, got ${ids.length}")
    assert(ids.toSet == (1 to 100).toSet, "Split boundaries must not lose or duplicate rows")
  }

  test("characterization: numPartitions on a single uncompressed file gives exact count") {
    val df = readFixedWidth("0:7,7:12", nameIdSchema,
      Map("numPartitions" -> "4"))(s"$testDataPath/large_test.txt")

    assert(df.rdd.getNumPartitions == 4,
      s"Expected exactly 4 partitions, got ${df.rdd.getNumPartitions}")
    assert(df.count() == 100)
  }

  // ===========================================================================
  // Phase 1: FileScan scaffolding units (new classes, not yet wired in)
  // ===========================================================================

  test("phase1: FixedWidthFileFormat V1 fallback shim throws loudly instead of reading or writing") {
    val fmt = new FixedWidthFileFormat()

    val readEx = intercept[UnsupportedOperationException] {
      fmt.inferSchema(spark, Map.empty[String, String], Seq.empty)
    }
    assert(readEx.getMessage.toLowerCase.contains("fixedwidth"),
      s"Shim error must be attributable to this source, got: ${readEx.getMessage}")

    val writeEx = intercept[UnsupportedOperationException] {
      fmt.prepareWrite(spark, org.apache.hadoop.mapreduce.Job.getInstance(),
        Map.empty[String, String], nameIdSchema)
    }
    assert(writeEx.getMessage.toLowerCase.contains("fixedwidth"),
      s"Shim error must be attributable to this source, got: ${writeEx.getMessage}")
  }

  test("phase1: new provider keeps the format short name and wires the throwing fallback") {
    val provider = new FixedWidthDataSourceV2()
    assert(provider.shortName() == "fixedwidth-custom-scala")
    assert(provider.supportsExternalMetadata())

    val options = new CaseInsensitiveStringMap(Map(
      "path" -> s"$testDataPath/valid1.txt",
      "field_lengths" -> "0:5,5:10"
    ).asJava)
    val table = provider.getTable(options)
    assert(table.isInstanceOf[org.apache.spark.sql.execution.datasources.v2.FileTable],
      "table must be built on Spark's FileTable machinery")
    assert(table.asInstanceOf[FixedWidthFileTable].fallbackFileFormat ==
      classOf[FixedWidthFileFormat])
  }

  test("phase1: FileTable-based table infers base schema and appends rescued column only") {
    val provider = new FixedWidthDataSourceV2()
    val options = new CaseInsensitiveStringMap(Map(
      "path" -> s"$testDataPath/valid1.txt",
      "field_lengths" -> "0:5,5:10",
      "rescuedDataColumn" -> "_rescued_data"
    ).asJava)

    val table = provider.getTable(options)
    val schema = table.schema()

    val baseNames = FWUtils.inferBaseSchema(options).fieldNames.toSeq
    assert(schema.fieldNames.take(baseNames.length).toSeq == baseNames,
      s"Inferred data columns must match FWUtils.inferBaseSchema, got ${schema.fieldNames.toSeq}")
    assert(schema.fieldNames.contains("_rescued_data"),
      "rescuedDataColumn must be auto-appended when the option is set")
    assert(!schema.fieldNames.contains("_corrupt_record"),
      "_corrupt_record must never be auto-appended")
  }

  test("phase1: FileTable-based table keeps user's _corrupt_record and auto-appends rescued column") {
    val provider = new FixedWidthDataSourceV2()
    val options = new CaseInsensitiveStringMap(Map(
      "path" -> s"$testDataPath/invalid1.txt",
      "field_lengths" -> "0:5,5:10",
      "columnNameOfCorruptRecord" -> "_corrupt_record",
      "rescuedDataColumn" -> "_rescued_data"
    ).asJava)
    val userSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true)
    ))

    val table = provider.getTable(options, userSchema)

    assert(table.schema().fieldNames.toSeq ==
      Seq("name", "id", "_corrupt_record", "_rescued_data"),
      s"Expected user schema + auto-appended rescued column, got ${table.schema().fieldNames.toSeq}")
  }

  // ===========================================================================
  // Phase 4: new capabilities gained from the FileScan machinery
  // ===========================================================================

  test("phase4: pathGlobFilter limits directory reads to matching files") {
    withTempDir { dir =>
      writeFile(dir, "a.txt", "Alice00001\n")
      writeFile(dir, "b.dat", "Bob  00002\n")

      val rows = readFixedWidth("0:5,5:10", nameIdSchema,
        Map("pathGlobFilter" -> "*.txt"))(dir.toString).collect()

      assert(rows.length == 1, s"pathGlobFilter=*.txt must exclude b.dat, got ${rows.length} rows")
      assert(rows(0).getAs[String]("name") == "Alice")
    }
  }

  test("phase4: recursiveFileLookup reads files in nested subdirectories") {
    withTempDir { dir =>
      val nested = Files.createDirectories(dir.resolve("nested").resolve("deeper"))
      writeFile(dir, "top.txt", "Alice00001\n")
      Files.write(nested.resolve("bottom.txt"), "Bob  00002\n".getBytes(StandardCharsets.UTF_8))

      val rows = readFixedWidth("0:5,5:10", nameIdSchema,
        Map("recursiveFileLookup" -> "true"))(dir.toString).collect()

      assert(rows.map(_.getAs[String]("name")).toSet == Set("Alice", "Bob"),
        s"recursiveFileLookup must find nested files, got ${rows.map(_.getString(0)).toSeq}")
    }
  }

  test("phase4: hive-style partition directories are inferred as partition columns") {
    withTempDir { dir =>
      val eu = Files.createDirectories(dir.resolve("region=EU"))
      val us = Files.createDirectories(dir.resolve("region=US"))
      Files.write(eu.resolve("part1.txt"), "Alice00001\n".getBytes(StandardCharsets.UTF_8))
      Files.write(us.resolve("part2.txt"), "Bob  00002\n".getBytes(StandardCharsets.UTF_8))

      val df = readFixedWidth("0:5,5:10", nameIdSchema)(dir.toString)

      assert(df.schema.fieldNames.contains("region"),
        s"Partition column must be appended to the schema, got ${df.schema.fieldNames.toSeq}")
      val rows = df.collect()
      assert(rows.map(r => (r.getAs[String]("name"), r.getAs[String]("region"))).toSet ==
        Set(("Alice", "EU"), ("Bob", "US")))
    }
  }

  test("phase4: explicit multi-path load keeps per-file error semantics (rescued per file, corrupt NULL)") {
    withTempDir { dir =>
      val good = writeFile(dir, "good.txt", "Alice00001\nBob  00002\n")
      val bad = writeFile(dir, "bad.txt", "Carol0000A\n")

      val schema = StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("_corrupt_record", StringType, nullable = true),
        StructField("_rescued_data", StringType, nullable = true)
      ))

      val rows = readFixedWidth("0:5,5:10", schema, Map(
        "mode" -> "PERMISSIVE",
        "columnNameOfCorruptRecord" -> "_corrupt_record",
        "rescuedDataColumn" -> "_rescued_data"
      ))(good.toString, bad.toString).collect()

      assert(rows.length == 3)
      val byName = rows.map(r => r.getAs[String]("name") -> r).toMap

      // Rows from the good file: both special columns NULL
      Seq("Alice", "Bob").foreach { n =>
        val r = byName(n)
        assert(r.isNullAt(r.fieldIndex("_corrupt_record")), s"$n: corrupt must be NULL")
        assert(r.isNullAt(r.fieldIndex("_rescued_data")), s"$n: rescued must be NULL")
      }

      // Bad row (from the other file): corrupt NULL, rescued JSON — identical to a single-file load
      val carol = byName("Carol")
      assert(carol.isNullAt(carol.fieldIndex("id")))
      assert(carol.isNullAt(carol.fieldIndex("_corrupt_record")),
        "corrupt must stay NULL when both options are set (rescued takes precedence)")
      val json = carol.getAs[String]("_rescued_data")
      assert(json != null && json.contains("\"id\"") && json.contains("0000A"))
    }
  }

  test("phase4: structurally misaligned files — multi-file load ≡ union of single-file loads") {
    // Verified target semantics (mirrors the CSV findings): wrong type, values in
    // undefined positions and completely wrong content all surface exactly as they
    // would in a per-file single load — the multi-file read adds no new behavior.
    withTempDir { dir =>
      val schema = StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("_corrupt_record", StringType, nullable = true),
        StructField("_rescued_data", StringType, nullable = true)
      ))

      val files = Seq(
        writeFile(dir, "good.txt", "Alice00001\n"),           // aligned
        writeFile(dir, "bad_type.txt", "Bob  IDNAN\n"),       // wrong type in id column
        writeFile(dir, "long_line.txt", "Carol00003XTRA##\n"), // content beyond defined positions
        writeFile(dir, "garbage.txt", "@@@@@@@@@@\n"),        // completely wrong columns/content
        writeFile(dir, "short_line.txt", "Eve\n")             // structurally shorter than the layout
      ).map(_.toString)

      def read(paths: String*) = readFixedWidth("0:5,5:10", schema, Map(
        "mode" -> "PERMISSIVE",
        "columnNameOfCorruptRecord" -> "_corrupt_record",
        "rescuedDataColumn" -> "_rescued_data"
      ))(paths: _*)

      val multi = read(files: _*).collect()
      val singles = files.flatMap(f => read(f).collect())

      // Core guarantee: the multi-file load is exactly the union of single-file loads
      assert(multi.length == 5 && singles.length == 5)
      assert(multi.map(_.toSeq).toSet == singles.map(_.toSeq).toSet,
        s"Multi-file result must equal per-file results.\nmulti:  ${multi.map(_.toSeq).toSeq}\nsingles: ${singles.map(_.toSeq).toSeq}")

      val byName = multi.map(r => r.getAs[String]("name") -> r).toMap

      // Aligned file: clean row, both special columns NULL
      assert(!byName("Alice").isNullAt(1) && byName("Alice").getAs[Int]("id") == 1)
      assert(byName("Alice").isNullAt(2) && byName("Alice").isNullAt(3))

      // Wrong type: id NULL, corrupt NULL (both options set), rescued has raw value
      val bob = byName("Bob")
      assert(bob.isNullAt(bob.fieldIndex("id")))
      assert(bob.isNullAt(bob.fieldIndex("_corrupt_record")))
      val bobJson = bob.getAs[String]("_rescued_data")
      assert(bobJson != null && bobJson.contains("\"id\"") && bobJson.contains("IDNAN"))

      // Content beyond the defined positions: ignored by design (fixed-width
      // positions define the columns) — defined fields parse cleanly
      val carol = byName("Carol")
      assert(carol.getAs[Int]("id") == 3)
      assert(carol.isNullAt(carol.fieldIndex("_corrupt_record")) &&
        carol.isNullAt(carol.fieldIndex("_rescued_data")))

      // Completely wrong content: typed column fails → rescued, corrupt stays NULL
      val garbage = byName("@@@@@")
      assert(garbage.isNullAt(garbage.fieldIndex("id")))
      assert(garbage.isNullAt(garbage.fieldIndex("_corrupt_record")))
      val garbageJson = garbage.getAs[String]("_rescued_data")
      assert(garbageJson != null && garbageJson.contains("\"id\"") && garbageJson.contains("@@@@@"))

      // Short line: missing trailing field → null, no corruption (fixed-width ≠ fixed-length)
      val eve = byName("Eve")
      assert(eve.isNullAt(eve.fieldIndex("id")))
      assert(eve.isNullAt(eve.fieldIndex("_corrupt_record")) &&
        eve.isNullAt(eve.fieldIndex("_rescued_data")))
    }
  }

  test("phase4: normal reads are served by the V2 path (throwing V1 fallback is never invoked)") {
    // FixedWidthFileFormat throws on any read or write; a plain read succeeding
    // proves the V1 fallback path is not taken for this source.
    val rows = readFixedWidth("0:5,5:10", nameIdSchema)(s"$testDataPath/valid1.txt").collect()
    assert(rows.length == 3)
  }

  test("phase4: reading a missing path fails with Spark's native path error") {
    val df = readFixedWidth("0:5,5:10", nameIdSchema)(s"$testDataPath/does_not_exist_12345.txt")
    val ex = intercept[Exception] {
      df.collect()
    }
    assert(ex.getMessage.contains("does_not_exist_12345"),
      s"Error must name the missing path, got: ${ex.getMessage}")
  }

  // ===========================================================================
  // 0b. Gap tests — fail against pre-migration code, pass from Phase 2 onward
  // ===========================================================================

  test("gap: explicit multi-path load(f1, f2) reads all listed files") {
    val rows = readFixedWidth("0:10,10:13", nameIdSchema)(
      s"$testDataPath/multi_file_a.txt",
      s"$testDataPath/multi_file_b.txt"
    ).collect()

    assert(rows.length == 6, s"Expected 6 rows from 2 explicitly listed files, got ${rows.length}")
    val names = rows.map(_.getAs[String]("name")).toSet
    assert(names.contains("FileA1") && names.contains("FileB3"),
      s"Expected rows from both files, got names: $names")
  }

  test("gap: input_file_name() returns the source file for every row") {
    val files = readFixedWidth("0:5,5:10", nameIdSchema)(s"$testDataPath/valid1.txt")
      .select(input_file_name())
      .collect()
      .map(_.getString(0))

    assert(files.length == 3)
    assert(files.forall(_.nonEmpty), "input_file_name() must not be empty for file-based rows")
    assert(files.forall(_.contains("valid1.txt")),
      s"input_file_name() must name the source file, got: ${files.toSeq.distinct}")
  }

  test("gap: _metadata.file_path is selectable and names the source file") {
    val rows = readFixedWidth("0:5,5:10", nameIdSchema)(s"$testDataPath/valid1.txt")
      .select(col("name"), col("_metadata.file_path"))
      .collect()

    assert(rows.length == 3)
    assert(rows.forall(_.getString(1).endsWith("valid1.txt")),
      s"_metadata.file_path must name the source file, got: ${rows.map(_.getString(1)).toSeq.distinct}")
  }

  test("gap: many small files bin-pack into far fewer partitions") {
    withTempDir { dir =>
      (1 to 40).foreach { i =>
        writeFile(dir, f"part_$i%02d.txt", f"Name$i%03d$i%05d\n")
      }

      // openCostInBytes=0 + minPartitionNum=1 => native maxSplitBytes = totalBytes,
      // so native bin-packing collapses all 40 tiny files into very few partitions.
      // The pre-migration planner ignores both confs and emits 1 partition per file.
      withSqlConf(
        "spark.sql.files.openCostInBytes" -> "0",
        "spark.sql.files.minPartitionNum" -> "1"
      ) {
        val df = readFixedWidth("0:7,7:12", nameIdSchema)(dir.toString)

        val ids = df.collect().map(_.getAs[Int]("id"))
        assert(ids.length == 40, s"Expected 40 rows (one per file), got ${ids.length}")
        assert(ids.toSet == (1 to 40).toSet)

        val numPartitions = df.rdd.getNumPartitions
        assert(numPartitions < 10,
          s"40 tiny files must bin-pack into few partitions, got $numPartitions")
      }
    }
  }

  test("gap: numPartitions on a multi-file load no longer disables splitting") {
    withTempDir { dir =>
      val largeBytes = Files.readAllBytes(Paths.get(s"$testDataPath/large_test.txt"))
      Files.write(dir.resolve("file1.txt"), largeBytes)
      Files.write(dir.resolve("file2.txt"), largeBytes)

      val df = readFixedWidth("0:7,7:12", nameIdSchema,
        Map("numPartitions" -> "4"))(dir.toString)

      assert(df.count() == 200, "Two copies of the 100-row file must yield 200 rows")
      val numPartitions = df.rdd.getNumPartitions
      assert(numPartitions > 2,
        "numPartitions=4 over two 1300-byte files must act as a global target " +
          s"(ceil(totalBytes/4) split size), not 1 unsplit partition per file; got $numPartitions")
    }
  }
}
