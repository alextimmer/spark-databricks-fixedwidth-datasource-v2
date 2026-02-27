// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
 * TDD test suite for missing functionality items M1-M7.
 *
 * Written BEFORE implementation (RED phase) per the Missing Functionality Analysis.
 * Each test targets a specific gap identified in docs/MISSING_FUNCTIONALITY_ANALYSIS.md:
 *
 *  - M2: Writer alignment (left-pad / right-align) support
 *  - M4: Writer lineEnding option
 *  - M5: Writer paddingChar option
 *  - M6: Writer DecimalType support
 *  - M7: Writer Date/Timestamp formatting
 *
 * M3 (CRLF byte counting) and M8 (constants consolidation) are internal
 * improvements without externally observable behavior changes.
 */
class MissingFunctionalitySpec extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("MissingFunctionalityTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val testDataPath: String = "data/test_data"

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def cleanup(path: String): Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(path), true)
  }

  private def readFirstLine(outputPath: String): String = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(new org.apache.hadoop.fs.Path(outputPath))
      .filter(f => f.getPath.getName.startsWith("part-") && f.getPath.getName.endsWith(".txt"))
    assert(files.nonEmpty, "No output part-*.txt files found")
    val stream = fs.open(files.head.getPath)
    val reader = new java.io.BufferedReader(new java.io.InputStreamReader(stream))
    val line = reader.readLine()
    reader.close()
    line
  }

  private def readRawBytes(outputPath: String): Array[Byte] = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(new org.apache.hadoop.fs.Path(outputPath))
      .filter(f => f.getPath.getName.startsWith("part-") && f.getPath.getName.endsWith(".txt"))
    assert(files.nonEmpty, "No output part-*.txt files found")
    val fileLen = files.head.getLen.toInt
    val stream = fs.open(files.head.getPath)
    val bytes = new Array[Byte](fileLen)
    stream.readFully(bytes)
    stream.close()
    bytes
  }

  // ===========================================================================
  // M5: Writer paddingChar option
  // ===========================================================================

  test("M5: writer pads fields with custom paddingChar instead of spaces") {
    val df = Seq(("Alice", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_m5_padchar"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("paddingChar", "0")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    assert(line.length == 15, s"Expected 15 chars, got ${line.length}: '$line'")
    // "Alice" left-aligned, padded with '0' to 10 chars
    assert(line.substring(0, 10) == "Alice00000",
      s"Expected 'Alice00000' but got '${line.substring(0, 10)}'")
    // "42" left-aligned, padded with '0' to 5 chars
    assert(line.substring(10, 15) == "42000",
      s"Expected '42000' but got '${line.substring(10, 15)}'")

    cleanup(outputPath)
  }

  // ===========================================================================
  // M2: Writer alignment option
  // ===========================================================================

  test("M2: writer right-aligns values when alignment is right") {
    val df = Seq(("Alice", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_m2_align"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("alignment", "right")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    assert(line.length == 15, s"Expected 15 chars, got ${line.length}: '$line'")
    // "Alice" right-aligned (left-padded with spaces) in 10 chars
    assert(line.substring(0, 10) == "     Alice",
      s"Expected '     Alice' but got '${line.substring(0, 10)}'")
    // "42" right-aligned (left-padded with spaces) in 5 chars
    assert(line.substring(10, 15) == "   42",
      s"Expected '   42' but got '${line.substring(10, 15)}'")

    cleanup(outputPath)
  }

  test("M5+M2: writer combines paddingChar with right alignment") {
    val df = Seq(("Alice", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_m5m2_combined"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("paddingChar", "0")
      .option("alignment", "right")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    assert(line.length == 15, s"Expected 15 chars, got ${line.length}: '$line'")
    // "Alice" right-aligned, zero-padded: "00000Alice"
    assert(line.substring(0, 10) == "00000Alice",
      s"Expected '00000Alice' but got '${line.substring(0, 10)}'")
    // "42" right-aligned, zero-padded: "00042"
    assert(line.substring(10, 15) == "00042",
      s"Expected '00042' but got '${line.substring(10, 15)}'")

    cleanup(outputPath)
  }

  test("M5+M2: roundtrip with zero-padded right-aligned integers preserves values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("amount", IntegerType)
    ))
    val data = Seq(Row(42, 7))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).coalesce(1)
    val outputPath = s"$testDataPath/output_m5m2_roundtrip"
    cleanup(outputPath)

    // Write right-aligned with zero padding
    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("paddingChar", "0")
      .option("alignment", "right")
      .mode("overwrite")
      .save(outputPath)

    // Verify raw output: "00042" and "00007"
    val line = readFirstLine(outputPath)
    assert(line == "0004200007",
      s"Expected '0004200007' but got '$line'")

    // Read back as IntegerType — handles leading zeros
    val readDf = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .schema(schema)
      .load(outputPath)

    val rows = readDf.collect()
    assert(rows.length == 1)
    assert(rows(0).getInt(0) == 42, s"Expected 42 but got ${rows(0).getInt(0)}")
    assert(rows(0).getInt(1) == 7, s"Expected 7 but got ${rows(0).getInt(1)}")

    cleanup(outputPath)
  }

  // ===========================================================================
  // M4: Writer lineEnding option
  // ===========================================================================

  test("M4: writer outputs CRLF when lineEnding option is set to CRLF") {
    val df = Seq(("Alice", 1), ("Bob", 2)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_m4_crlf"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("lineEnding", "\r\n")
      .mode("overwrite")
      .save(outputPath)

    val rawBytes = readRawBytes(outputPath)
    val content = new String(rawBytes, "UTF-8")

    // Count CRLF sequences
    val crlfCount = content.sliding(2).count(_ == "\r\n")
    // Every LF must be preceded by CR (no lone LFs)
    val totalLf = content.count(_ == '\n')

    assert(crlfCount >= 2,
      s"Expected at least 2 CRLF endings, got $crlfCount. Raw hex: ${rawBytes.take(60).map(b => f"$b%02X").mkString(" ")}")
    assert(totalLf == crlfCount,
      s"Found lone LF without CR: $totalLf total LFs but only $crlfCount CRLFs")

    cleanup(outputPath)
  }

  // ===========================================================================
  // M7: Writer Date/Timestamp formatting
  // ===========================================================================

  test("M7: writer formats DateType as human-readable date string") {
    import java.sql.Date
    val df = Seq(("Alice", Date.valueOf("2023-01-15"))).toDF("name", "birth_date").coalesce(1)
    val outputPath = s"$testDataPath/output_m7_date"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20")
      .option("dateFormat", "yyyy-MM-dd")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    val dateStr = line.substring(10, 20).trim
    // Must be a formatted date string, not an epoch integer like "19372"
    assert(dateStr.matches("\\d{4}-\\d{2}-\\d{2}"),
      s"Expected formatted date 'yyyy-MM-dd' but got '$dateStr'")
    assert(dateStr == "2023-01-15",
      s"Expected '2023-01-15' but got '$dateStr'")

    cleanup(outputPath)
  }

  test("M7: writer formats TimestampType as human-readable timestamp string") {
    import java.sql.Timestamp
    import java.time.Instant
    // Use explicit UTC instant to avoid timezone ambiguity
    val ts = Timestamp.from(Instant.parse("2023-01-15T10:30:00Z"))
    val df = Seq(("Alice", ts)).toDF("name", "created_at").coalesce(1)
    val outputPath = s"$testDataPath/output_m7_ts"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:29")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("timeZone", "UTC")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    val tsStr = line.substring(10, 29).trim
    // Must be formatted, not epoch microseconds like "1673778600000000"
    assert(tsStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"),
      s"Expected formatted timestamp 'yyyy-MM-dd HH:mm:ss' but got '$tsStr'")
    assert(tsStr == "2023-01-15 10:30:00",
      s"Expected '2023-01-15 10:30:00' but got '$tsStr'")

    cleanup(outputPath)
  }

  test("M7: write-read roundtrip preserves Date and Timestamp values") {
    import java.sql.{Date, Timestamp}
    import java.time.Instant

    val d1 = Date.valueOf("2023-01-15")
    val d2 = Date.valueOf("2023-12-31")
    val ts1 = Timestamp.from(Instant.parse("2023-01-15T10:30:00Z"))
    val ts2 = Timestamp.from(Instant.parse("2023-12-31T23:59:59Z"))

    val df = Seq(
      ("Alice", d1, ts1),
      ("Bob", d2, ts2)
    ).toDF("name", "birth_date", "created_at").coalesce(1)
    val outputPath = s"$testDataPath/output_m7_roundtrip"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20,20:39")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("timeZone", "UTC")
      .mode("overwrite")
      .save(outputPath)

    val readSchema = StructType(Seq(
      StructField("name", StringType),
      StructField("birth_date", DateType),
      StructField("created_at", TimestampType)
    ))
    val readDf = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20,20:39")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("timeZone", "UTC")
      .schema(readSchema)
      .load(outputPath)

    // Use SQL cast to avoid JDK 17+ module access issues with DateType deserialization
    val rows = readDf
      .selectExpr("name", "CAST(birth_date AS STRING) as bd", "CAST(created_at AS STRING) as ca")
      .collect()
      .sortBy(_.getString(0))
    assert(rows.length == 2)

    val aliceDateStr = rows(0).getAs[String]("bd")
    assert(aliceDateStr == "2023-01-15",
      s"Date roundtrip failed: expected '2023-01-15' but got '$aliceDateStr'")

    val bobDateStr = rows(1).getAs[String]("bd")
    assert(bobDateStr == "2023-12-31",
      s"Date roundtrip failed: expected '2023-12-31' but got '$bobDateStr'")

    cleanup(outputPath)
  }

  // ===========================================================================
  // M6: Writer DecimalType support
  // ===========================================================================

  test("M6: writer formats DecimalType with proper scale") {
    import org.apache.spark.sql.functions._
    val df = Seq("Apple").toDF("name")
      .withColumn("price", lit(new java.math.BigDecimal("42.50")).cast(DecimalType(10, 2)))
      .coalesce(1)
    val outputPath = s"$testDataPath/output_m6_decimal"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    val priceStr = line.substring(10, 20).trim
    // Must contain a human-readable decimal, not internal representation
    assert(priceStr.contains("42.5"),
      s"Expected decimal containing '42.5' but got '$priceStr'")

    // Roundtrip: read back as DecimalType
    val readSchema = StructType(Seq(
      StructField("name", StringType),
      StructField("price", DecimalType(10, 2))
    ))
    val readDf = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20")
      .schema(readSchema)
      .load(outputPath)

    val rows = readDf.collect()
    assert(rows.length == 1)
    val readPrice = rows(0).getDecimal(1)
    assert(readPrice.doubleValue() == 42.50,
      s"Decimal roundtrip failed: expected 42.50 but got $readPrice")

    cleanup(outputPath)
  }

  // ===========================================================================
  // M8: Writer constants consolidation (compile-time verification)
  // ===========================================================================

  test("M8: FixedWidthConstants contains writer-relevant option keys") {
    // Verify that all writer options are centralized in FixedWidthConstants
    assert(FixedWidthConstants.OptionKeys.PATH == "path",
      "PATH constant should be defined")
    assert(FixedWidthConstants.OptionKeys.ENCODING == "encoding",
      "ENCODING constant should be defined")
    assert(FixedWidthConstants.OptionKeys.FIELD_LENGTHS == "field_lengths",
      "FIELD_LENGTHS constant should be defined")

    // New constants for M2, M4, M5 — these will fail until constants are added
    assert(FixedWidthConstants.OptionKeys.PADDING_CHAR == "paddingChar",
      "PADDING_CHAR constant should be defined")
    assert(FixedWidthConstants.OptionKeys.ALIGNMENT == "alignment",
      "ALIGNMENT constant should be defined")
    assert(FixedWidthConstants.OptionKeys.LINE_ENDING == "lineEnding",
      "LINE_ENDING constant should be defined")
  }
}
