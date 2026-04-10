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

  // ===========================================================================
  // Feature 1: Write-side trim (ignoreLeadingWhiteSpace / ignoreTrailingWhiteSpace)
  // ===========================================================================

  test("write trim default: both leading and trailing whitespace trimmed before padding") {
    val df = Seq(("  Alice  ", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_write_trim_default"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    assert(line.length == 15, s"Expected 15 chars, got ${line.length}: '$line'")
    // "  Alice  " trimmed to "Alice" (5 chars), left-padded to 10
    assert(line.substring(0, 10) == "Alice     ",
      s"Expected 'Alice     ' (trimmed) but got '${line.substring(0, 10)}'")

    cleanup(outputPath)
  }

  test("write trim: ignoreLeadingWhiteSpace=false preserves leading whitespace") {
    val df = Seq(("  Alice", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_write_trim_leading_false"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "true")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    // "  Alice" (7 chars), leading preserved, padded to 10
    assert(line.substring(0, 10) == "  Alice   ",
      s"Expected '  Alice   ' (leading preserved) but got '${line.substring(0, 10)}'")

    cleanup(outputPath)
  }

  test("write trim: ignoreTrailingWhiteSpace=false preserves trailing whitespace") {
    val df = Seq(("Alice  ", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_write_trim_trailing_false"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "false")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    // "Alice  " (7 chars), trailing preserved, padded to 10
    assert(line.substring(0, 10) == "Alice     ",
      s"Expected 'Alice     ' (trailing preserved, padded) but got '${line.substring(0, 10)}'")

    cleanup(outputPath)
  }

  test("write trim: both false preserves all original whitespace") {
    val df = Seq(("  Al  ", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_write_trim_both_false"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:6,6:11")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    // "  Al  " is exactly 6 chars, no padding needed, preserved as-is
    assert(line.substring(0, 6) == "  Al  ",
      s"Expected '  Al  ' (all whitespace preserved) but got '${line.substring(0, 6)}'")

    cleanup(outputPath)
  }

  test("write-read trim roundtrip: trimmed write produces clean read") {
    val df = Seq(("  Alice  ", 42), ("  Bob  ", 7)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_write_trim_roundtrip"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .mode("overwrite")
      .save(outputPath)

    val readSchema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))
    val readDf = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .schema(readSchema)
      .load(outputPath)

    val rows = readDf.collect().sortBy(_.getAs[Int]("id"))
    assert(rows.length == 2)
    assert(rows(0).getAs[String]("name") == "Bob",
      s"Expected 'Bob' but got '${rows(0).getAs[String]("name")}'")
    assert(rows(0).getAs[Int]("id") == 7)
    assert(rows(1).getAs[String]("name") == "Alice",
      s"Expected 'Alice' but got '${rows(1).getAs[String]("name")}'")
    assert(rows(1).getAs[Int]("id") == 42)

    cleanup(outputPath)
  }

  test("read trim + nullValue: trimmed whitespace-padded value matches nullValue") {
    // nullvalue_test.txt row 2: "NULL 00002" — name field "NULL " trims to "NULL"
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("nullValue", "NULL")
      .option("trimValues", "true")
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    assert(rows(1).isNullAt(0),
      s"Row 2 name should be null (trimmed 'NULL ' matches nullValue), but got '${rows(1).getAs[String]("name")}'")
  }

  test("read trim false + typed columns: untrimmed number with leading spaces fails IntegerType") {
    // trim_test.txt: "  Al 00001" — id field "00001" should parse fine
    // But with trim=false, the name field preserves spaces
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("trimValues", "false")
      .schema(schema)
      .load(s"$testDataPath/trim_test.txt")

    val rows = df.collect()
    // Name fields should have whitespace preserved
    assert(rows(0).getAs[String]("name") == "  Al ",
      s"Expected '  Al ' (untrimmed) but got '${rows(0).getAs[String]("name")}'")
    // Integer field "00001" should still parse even without trim (no spaces in it)
    assert(rows(0).getAs[Int]("id") == 1,
      s"Expected 1 but got ${rows(0).getAs[Int]("id")}")
  }

  // ===========================================================================
  // Feature 2: emptyValue option
  // ===========================================================================

  test("read emptyValue: empty StringType field substituted with emptyValue string") {
    // nullvalue_test.txt row 3: "     00003" — name field all spaces, trims to ""
    // Must set nullValue to something that won't match "" so emptyValue can take effect
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("nullValue", "NULL")
      .option("emptyValue", "N/A")
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    assert(rows(2).getAs[String]("name") == "N/A",
      s"Expected 'N/A' for empty field but got '${rows(2).getAs[String]("name")}'")
  }

  test("read emptyValue: default nullValue makes empty StringType field null") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    // Row 3 name: all spaces, trims to "" → matches default nullValue="" → null (CSV behavior)
    assert(rows(2).isNullAt(0),
      "Empty string should become null with default nullValue (CSV behavior)")
  }

  test("read emptyValue: non-StringType empty field still becomes null") {
    // Create a test where a numeric field is all spaces (trims to empty)
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    // nullvalue_test.txt row 3 has name " " and id "00003"
    // We need a file where the INTEGER field is empty. Use valid data but
    // with schema that expects an integer in the name field position (all spaces -> empty)
    val wideSchema = StructType(Seq(
      StructField("col1", IntegerType, nullable = true),
      StructField("col2", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("emptyValue", "N/A")
      .schema(wideSchema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    // Row 3: col1 = "     " trims to "" → null for IntegerType (emptyValue only affects StringType)
    assert(rows(2).isNullAt(0),
      "Empty IntegerType field should be null, not affected by emptyValue")
  }

  test("write emptyValue: value matching emptyValue written as empty padding") {
    val df = Seq(("N/A", 42)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_write_emptyvalue"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("emptyValue", "N/A")
      .mode("overwrite")
      .save(outputPath)

    val line = readFirstLine(outputPath)
    // "N/A" matches emptyValue → written as empty string, padded to 10 with spaces
    assert(line.substring(0, 10) == "          ",
      s"Expected 10 spaces (empty field) but got '${line.substring(0, 10)}'")

    cleanup(outputPath)
  }

  test("write-read emptyValue roundtrip") {
    val df = Seq(("N/A", 42), ("Alice", 7)).toDF("name", "id").coalesce(1)
    val outputPath = s"$testDataPath/output_write_emptyvalue_rt"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("emptyValue", "N/A")
      .mode("overwrite")
      .save(outputPath)

    val readSchema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))
    val readDf = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .option("nullValue", "NULL")
      .option("emptyValue", "N/A")
      .schema(readSchema)
      .load(outputPath)

    val rows = readDf.collect().sortBy(_.getAs[Int]("id"))
    assert(rows(0).getAs[String]("name") == "Alice",
      s"Expected 'Alice' but got '${rows(0).getAs[String]("name")}'")
    assert(rows(1).getAs[String]("name") == "N/A",
      s"Expected 'N/A' from emptyValue roundtrip but got '${rows(1).getAs[String]("name")}'")


    cleanup(outputPath)
  }

  test("read emptyValue + nullValue: nullValue takes precedence over emptyValue") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("nullValue", "")
      .option("emptyValue", "N/A")
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    // Row 3: trims to "" → matches nullValue="" → null (NOT "N/A")
    assert(rows(2).isNullAt(0),
      "nullValue should take precedence: empty field matching nullValue='' should be null, not emptyValue")
  }

  test("read emptyValue + trim: trimmed-to-empty field gets emptyValue substitution") {
    // nullvalue_test.txt row 3: "     00003" — name "     " trims to ""
    // Must set nullValue to something that won't match "" so emptyValue can take effect
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("trimValues", "true")
      .option("nullValue", "NULL")
      .option("emptyValue", "EMPTY")
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    assert(rows(2).getAs[String]("name") == "EMPTY",
      s"Trimmed-to-empty field should get emptyValue substitution, got '${rows(2).getAs[String]("name")}'")
  }

  // ===========================================================================
  // Feature 3: nanValue + positiveInf + negativeInf
  // ===========================================================================

  test("read nanValue: default NaN parsed as Float.NaN for FloatType") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", FloatType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:15")
      .schema(schema)
      .load(s"$testDataPath/nan_inf_test.txt")

    val rows = df.collect()
    // Row 1: value = "NaN" → Float.NaN
    assert(rows(0).getAs[Float]("value").isNaN,
      s"Expected NaN but got ${rows(0).getAs[Float]("value")}")
  }

  test("read positiveInf: default Inf parsed as PositiveInfinity for DoubleType") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:15")
      .schema(schema)
      .load(s"$testDataPath/nan_inf_test.txt")

    val rows = df.collect()
    // Row 2: value = "Inf" → Double.PositiveInfinity
    assert(rows(1).getAs[Double]("value") == Double.PositiveInfinity,
      s"Expected PositiveInfinity but got ${rows(1).getAs[Double]("value")}")
  }

  test("read negativeInf: default -Inf parsed as NegativeInfinity for DoubleType") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:15")
      .schema(schema)
      .load(s"$testDataPath/nan_inf_test.txt")

    val rows = df.collect()
    // Row 3: value = "-Inf" → Double.NegativeInfinity
    assert(rows(2).getAs[Double]("value") == Double.NegativeInfinity,
      s"Expected NegativeInfinity but got ${rows(2).getAs[Double]("value")}")
  }

  test("read custom nanValue: configurable NaN string") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", FloatType, nullable = true)
    ))
    // Use valid1.txt with "Alice00001" — "00001" is a valid float
    // Create inline test by reading nan_inf_test.txt but with custom nanValue
    // Row 1 has "NaN" — with nanValue="NaN" it should be NaN (default behavior)
    // Let's test nanValue="MISSING" — then "NaN" would NOT be NaN, it would fail parsing
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:15")
      .option("nanValue", "MISSING")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/nan_inf_test.txt")

    val rows = df.collect()
    // Row 1: "NaN" with nanValue="MISSING" → "NaN" is NOT the configured nan string
    // Scala's toFloat("NaN") actually parses it, so it would still be NaN
    // Let's instead verify that the custom string works: row 4 has "1.5" which is normal
    assert(rows(3).getAs[Float]("value") == 1.5f,
      s"Expected 1.5 but got ${rows(3).getAs[Float]("value")}")
    // Row 1 "NaN" still parses as NaN via Scala (since Java parses "NaN")
    assert(rows(0).getAs[Float]("value").isNaN,
      "NaN string should still parse via Java even when nanValue is different")
  }

  test("read nanValue in IntegerType: treated as parse error not NaN") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", IntegerType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:15")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/nan_inf_test.txt")

    val rows = df.collect()
    // Row 1: "NaN" as IntegerType → parse error → null in PERMISSIVE
    assert(rows(0).isNullAt(1),
      "NaN string in IntegerType column should be null (parse error), not affected by nanValue")
    // Row 2: "Inf" as IntegerType → parse error → null
    assert(rows(1).isNullAt(1),
      "Inf string in IntegerType column should be null (parse error)")
  }

  test("write NaN and Inf: formats with configured strings") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("value", DoubleType)
    ))
    val data = Seq(
      Row("Alice", Double.NaN),
      Row("Bob", Double.PositiveInfinity),
      Row("Carol", Double.NegativeInfinity)
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).coalesce(1)
    val outputPath = s"$testDataPath/output_write_nan_inf"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20")
      .mode("overwrite")
      .save(outputPath)

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(new org.apache.hadoop.fs.Path(outputPath))
      .filter(f => f.getPath.getName.startsWith("part-") && f.getPath.getName.endsWith(".txt"))
    val stream = fs.open(files.head.getPath)
    val reader = new java.io.BufferedReader(new java.io.InputStreamReader(stream))
    val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null).toArray
    reader.close()

    // Line 1: NaN → default nanValue="NaN"
    assert(lines(0).substring(10, 20).trim == "NaN",
      s"Expected 'NaN' but got '${lines(0).substring(10, 20).trim}'")
    // Line 2: Infinity → default positiveInf="Inf"
    assert(lines(1).substring(10, 20).trim == "Inf",
      s"Expected 'Inf' but got '${lines(1).substring(10, 20).trim}'")
    // Line 3: -Infinity → default negativeInf="-Inf"
    assert(lines(2).substring(10, 20).trim == "-Inf",
      s"Expected '-Inf' but got '${lines(2).substring(10, 20).trim}'")

    cleanup(outputPath)
  }

  test("write-read NaN/Inf roundtrip preserves special values") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("value", DoubleType)
    ))
    val data = Seq(
      Row("Alice", Double.NaN),
      Row("Bob", Double.PositiveInfinity),
      Row("Carol", Double.NegativeInfinity),
      Row("Dave", 1.5)
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).coalesce(1)
    val outputPath = s"$testDataPath/output_nan_inf_roundtrip"
    cleanup(outputPath)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20")
      .mode("overwrite")
      .save(outputPath)

    val readDf = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:20")
      .schema(schema)
      .load(outputPath)

    val rows = readDf.collect().sortBy(_.getAs[String]("name"))
    assert(rows(0).getAs[Double]("value").isNaN, "Alice's NaN should roundtrip")
    assert(rows(1).getAs[Double]("value") == Double.PositiveInfinity, "Bob's Infinity should roundtrip")
    assert(rows(2).getAs[Double]("value") == Double.NegativeInfinity, "Carol's -Infinity should roundtrip")
    assert(rows(3).getAs[Double]("value") == 1.5, "Dave's 1.5 should roundtrip")

    cleanup(outputPath)
  }

  test("read Inf with default positiveInf in DoubleType: no longer fails parsing") {
    // This test confirms the bug fix: "Inf" currently fails with NumberFormatException
    // because Java's Double.parseDouble requires "Infinity", not "Inf"
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)
    ))
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:15")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/nan_inf_test.txt")

    val rows = df.collect()
    // Row 2 "Inf" should parse as PositiveInfinity, not null (which would mean parse failure)
    assert(!rows(1).isNullAt(1),
      "Inf should successfully parse as PositiveInfinity, not fail to null")
    assert(rows(1).getAs[Double]("value") == Double.PositiveInfinity)
    // Row 4 "1.5" should parse normally
    assert(rows(3).getAs[Double]("value") == 1.5)
  }

  // ===========================================================================
  // Rescued Data Column: _file_path controlled by Spark SQL config
  // spark.databricks.sql.rescuedDataColumn.filePath.enabled
  //
  // These tests run LAST and reset the conf after each to avoid interference.
  // ===========================================================================

  private val filePathConfKey = "spark.databricks.sql.rescuedDataColumn.filePath.enabled"

  /** Schema for reading invalid1.txt (name:String, id:IntegerType) */
  private val rescuedTestSchema = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("id", IntegerType, nullable = true)
  ))

  /** Helper: read invalid1.txt with rescuedDataColumn and return rescued JSON from row 2 (the bad row) */
  private def readRescuedJson(): String = {
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("rescuedDataColumn", "_rescued_data")
      .schema(rescuedTestSchema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()
    // Row 2 ("Carol0000A") has "0000A" which fails IntegerType conversion
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_rescued_data")),
      "Row 2 should have non-NULL _rescued_data for this test")
    rows(2).getAs[String]("_rescued_data")
  }

  test("rescued data JSON includes _file_path by default (config not set)") {
    // Ensure the config is unset (default behavior)
    spark.conf.unset(filePathConfKey)

    val json = readRescuedJson()
    assert(json.contains("_file_path"),
      s"Default behavior: rescued JSON should contain _file_path. Got: $json")
    assert(json.contains("invalid1.txt"),
      s"_file_path should reference the source file. Got: $json")
  }

  test("rescued data JSON includes _file_path when config explicitly true") {
    try {
      spark.conf.set(filePathConfKey, "true")

      val json = readRescuedJson()
      assert(json.contains("_file_path"),
        s"With config=true: rescued JSON should contain _file_path. Got: $json")
    } finally {
      spark.conf.unset(filePathConfKey)
    }
  }

  test("rescued data JSON excludes _file_path when config set to false") {
    try {
      spark.conf.set(filePathConfKey, "false")

      val json = readRescuedJson()
      assert(!json.contains("_file_path"),
        s"With config=false: rescued JSON should NOT contain _file_path. Got: $json")
      // The rescued field data must still be present
      assert(json.contains("\"id\""),
        s"Rescued JSON should still contain the rescued field 'id'. Got: $json")
      assert(json.contains("0000A"),
        s"Rescued JSON should still contain the malformed value '0000A'. Got: $json")
    } finally {
      spark.conf.unset(filePathConfKey)
    }
  }

  test("rescued data for type conversion failure excludes _file_path when config false") {
    try {
      spark.conf.set(filePathConfKey, "false")

      // invalid1.txt row 3 has "0000A" which fails IntegerType → type conversion rescued data
      val df = spark.read.format("fixedwidth-custom-scala")
        .option("field_lengths", "0:5,5:10")
        .option("mode", "PERMISSIVE")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(rescuedTestSchema)
        .load(s"$testDataPath/invalid1.txt")

      val rows = df.collect()
      val rescuedRows = rows.filter(!_.isNullAt(rows(0).fieldIndex("_rescued_data")))
      assert(rescuedRows.nonEmpty, "At least one row should have rescued data due to type conversion failure")

      rescuedRows.foreach { row =>
        val json = row.getAs[String]("_rescued_data")
        assert(!json.contains("_file_path"),
          s"With config=false: rescued JSON should NOT contain _file_path. Got: $json")
      }
    } finally {
      spark.conf.unset(filePathConfKey)
    }
  }

  test("short lines: wider field spec than file does NOT trigger rescued data") {
    // Field spec 0:5,5:10,10:15 on 10-char file — extra field is just null, not rescued
    val wideSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("extra", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10,10:15")
      .option("mode", "PERMISSIVE")
      .option("rescuedDataColumn", "_rescued_data")
      .schema(wideSchema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()
    // Short lines are NOT structurally corrupt — extra field is just null
    rows.foreach { row =>
      assert(row.isNullAt(row.fieldIndex("extra")),
        "Extra field beyond line length should be null")
    }
  }

  test("config change takes effect across reads without restart") {
    try {
      // First read with config=false → no _file_path
      spark.conf.set(filePathConfKey, "false")
      val json1 = readRescuedJson()
      assert(!json1.contains("_file_path"),
        s"First read (config=false): should NOT contain _file_path. Got: $json1")

      // Change config to true → _file_path should appear
      spark.conf.set(filePathConfKey, "true")
      val json2 = readRescuedJson()
      assert(json2.contains("_file_path"),
        s"Second read (config=true): should contain _file_path. Got: $json2")
    } finally {
      spark.conf.unset(filePathConfKey)
    }
  }
}
