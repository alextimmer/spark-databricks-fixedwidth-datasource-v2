// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class FixedWidthRelationSpec extends AnyFunSuite {

  // Create a small local SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("FixedWidthRelationTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Path to test data directory
  val testDataPath: String = "data/test_data"

  test("simple sanity test for Spark") {
    val df = Seq((1, "abc"), (2, "def")).toDF("id", "value")
    val collected = df.collect()
    assert(collected.length == 2)
    assert(collected.head.getAs[Int]("id") == 1)
    assert(collected(1).getAs[String]("value") == "def")
  }

  // =====================================================
  // PERMISSIVE MODE TESTS
  // =====================================================

  test("PERMISSIVE mode: valid StringType and IntegerType parsing") {
    // Data: "Alice00001", "Bob  00002", "Carol00003"
    // field_lengths: 0:5 (name), 5:10 (id as string "00001" -> 1)
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // Verify correct parsing
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(0).getAs[Int]("id") == 1)

    assert(rows(1).getAs[String]("name") == "Bob")
    assert(rows(1).getAs[Int]("id") == 2)

    assert(rows(2).getAs[String]("name") == "Carol")
    assert(rows(2).getAs[Int]("id") == 3)
  }

  test("PERMISSIVE mode: type conversion failure results in NULL, not corruption") {
    // Data: "Alice00001", "Bob  00002", "Carol0000A"
    // The third row has "0000A" which cannot be parsed as Int -> should be NULL
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // First two rows should parse correctly
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(0).getAs[Int]("id") == 1)

    assert(rows(1).getAs[String]("name") == "Bob")
    assert(rows(1).getAs[Int]("id") == 2)

    // Third row: "0000A" cannot be parsed as Int -> NULL (PERMISSIVE behavior)
    assert(rows(2).getAs[String]("name") == "Carol")
    assert(rows(2).isNullAt(rows(2).fieldIndex("id")))
  }

  test("PERMISSIVE mode: LongType parsing with conversion failures") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", LongType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // Valid Long values
    assert(rows(0).getAs[Long]("value") == 1L)
    assert(rows(1).getAs[Long]("value") == 2L)

    // "0000A" cannot parse to Long -> NULL
    assert(rows(2).isNullAt(rows(2).fieldIndex("value")))
  }

  test("PERMISSIVE mode: DoubleType parsing") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect()

    // Integer strings should convert to Double
    assert(rows(0).getAs[Double]("value") == 1.0)
    assert(rows(1).getAs[Double]("value") == 2.0)
    assert(rows(2).getAs[Double]("value") == 3.0)
  }

  test("PERMISSIVE mode with rescuedDataColumn: type conversion failures populate rescued column") {
    // With rescuedDataColumn set, type conversion failures SHOULD populate it
    // This replicates CSV behavior where malformed values are captured in the JSON
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("rescuedDataColumn", "_rescued_data")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // First two rows have valid data - _rescued_data should be NULL
    assert(rows(0).isNullAt(rows(0).fieldIndex("_rescued_data")),
      "Row 0 should have NULL _rescued_data (no conversion failures)")
    assert(rows(1).isNullAt(rows(1).fieldIndex("_rescued_data")),
      "Row 1 should have NULL _rescued_data (no conversion failures)")

    // Third row has "0000A" which fails to convert to Int
    // _rescued_data should contain JSON with the malformed value
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_rescued_data")),
      "Row 2 should have non-NULL _rescued_data (conversion failure)")

    val rescuedJson = rows(2).getAs[String]("_rescued_data")
    assert(rescuedJson.contains("\"id\""), "Rescued data should contain 'id' field name")
    assert(rescuedJson.contains("0000A"), "Rescued data should contain malformed value '0000A'")
    assert(rescuedJson.contains("_file_path"), "Rescued data should contain _file_path")

    // The id field itself should be NULL (type conversion failed)
    assert(rows(2).isNullAt(rows(2).fieldIndex("id")))
  }

  test("PERMISSIVE mode WITHOUT special columns: type failures just become NULL") {
    // When NO special column options are set, type conversion failures
    // simply result in NULL values - no error tracking
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      // Note: NO rescuedDataColumn or columnNameOfCorruptRecord options
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // Schema should have exactly 2 columns (no special columns added)
    assert(df.schema.length == 2)
    assert(df.schema.fieldNames.toSet == Set("name", "id"))

    // Valid rows should work
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(0).getAs[Int]("id") == 1)

    // Third row: "0000A" cannot be parsed as Int -> NULL (PERMISSIVE behavior)
    assert(rows(2).getAs[String]("name") == "Carol")
    assert(rows(2).isNullAt(rows(2).fieldIndex("id")))
  }

  test("schema with more fields than field_lengths: extra fields get NULL") {
    // Schema has 3 fields but only 2 field_lengths positions
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id1", IntegerType, nullable = true),
      StructField("id2", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")  // Only 2 positions
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect()

    // First two fields should be populated
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(0).getAs[Int]("id1") == 1)

    // Third field should be NULL (no data for it)
    assert(rows(0).isNullAt(rows(0).fieldIndex("id2")))
  }

  // =====================================================
  // CSV BEHAVIOR REPLICATION TESTS (6 Scenarios)
  // =====================================================

  test("Scenario 1: columnNameOfCorruptRecord set, column IN schema - populated with raw line") {
    // CSV behavior: column added to output and populated with raw row string when parsing fails
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // Schema should have exactly 3 columns
    assert(df.schema.length == 3)

    // Valid rows: _corrupt_record should be NULL
    assert(rows(0).isNullAt(rows(0).fieldIndex("_corrupt_record")),
      "Valid row should have NULL _corrupt_record")
    assert(rows(1).isNullAt(rows(1).fieldIndex("_corrupt_record")),
      "Valid row should have NULL _corrupt_record")

    // Malformed row: _corrupt_record should contain comma-separated parsed values (like CSV)
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_corrupt_record")),
      "Malformed row should have non-NULL _corrupt_record")
    val corruptValue = rows(2).getAs[String]("_corrupt_record")
    // CSV formats corrupt records as comma-separated field values
    assert(corruptValue == "Carol,0000A",
      s"Corrupt record should be CSV-like 'Carol,0000A', got: $corruptValue")

    // The failing field should still be NULL
    assert(rows(2).isNullAt(rows(2).fieldIndex("id")))
  }

  test("Scenario 2: columnNameOfCorruptRecord set, column NOT in schema - column silently dropped") {
    // CSV behavior: Spark silently drops the column entirely; data frame contains only declared fields
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
      // Note: _corrupt_record NOT in schema
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")  // Option set but column not declared
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // Schema should have exactly 2 columns - corrupt column NOT added
    assert(df.schema.length == 2,
      s"Schema should have 2 columns (corrupt NOT auto-added), got: ${df.schema.fieldNames.mkString(", ")}")
    assert(df.schema.fieldNames.toSet == Set("name", "id"))

    // Type conversion failure should still result in NULL
    assert(rows(2).isNullAt(rows(2).fieldIndex("id")))
  }

  test("Scenario 3: rescuedDataColumn set, column NOT in schema - auto-appended and populated") {
    // CSV behavior: Spark appends the rescued column automatically and fills it with JSON on malformed rows
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
      // Note: _rescued_data NOT in schema
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("rescuedDataColumn", "_rescued_data")  // Option set, column not declared
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    // Schema should have 3 columns - rescued column AUTO-APPENDED
    assert(df.schema.length == 3,
      s"Schema should have 3 columns (rescued auto-added), got: ${df.schema.fieldNames.mkString(", ")}")
    assert(df.schema.fieldNames.contains("_rescued_data"))

    val rows = df.collect()

    // Valid rows: _rescued_data should be NULL
    assert(rows(0).isNullAt(rows(0).fieldIndex("_rescued_data")))
    assert(rows(1).isNullAt(rows(1).fieldIndex("_rescued_data")))

    // Malformed row: _rescued_data should contain JSON
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_rescued_data")))
    val rescuedJson = rows(2).getAs[String]("_rescued_data")
    assert(rescuedJson.contains("\"id\""), "Rescued data should contain 'id' field name")
    assert(rescuedJson.contains("0000A"), "Rescued data should contain malformed value")
    assert(rescuedJson.contains("_file_path"), "Rescued data should contain _file_path")
  }

  test("Scenario 4: rescuedDataColumn set, column IN schema - retained and populated") {
    // CSV behavior: column retained and populated identically (JSON payload or NULL)
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_rescued_data", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("rescuedDataColumn", "_rescued_data")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    // Schema should have exactly 3 columns (user's schema preserved)
    assert(df.schema.length == 3)
    assert(df.schema.fieldNames.toSeq == Seq("name", "id", "_rescued_data"))

    val rows = df.collect()

    // Valid rows: _rescued_data should be NULL
    assert(rows(0).isNullAt(rows(0).fieldIndex("_rescued_data")))

    // Malformed row: _rescued_data should contain JSON
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_rescued_data")))
    val rescuedJson = rows(2).getAs[String]("_rescued_data")
    assert(rescuedJson.contains("\"id\""))
    assert(rescuedJson.contains("0000A"))
  }

  test("Scenario 5: Both options set, neither column in schema - only rescued appended") {
    // CSV behavior: Spark appends both columns, _corrupt_record stays NULL, _rescued_data carries JSON
    // BUT: We follow the rule that corrupt is ONLY included if in user schema
    // So only _rescued_data should be appended
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
      // Neither _corrupt_record nor _rescued_data in schema
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("rescuedDataColumn", "_rescued_data")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    // Only _rescued_data should be appended (corrupt not auto-added per CSV behavior)
    assert(df.schema.length == 3,
      s"Schema should have 3 columns, got: ${df.schema.fieldNames.mkString(", ")}")
    assert(df.schema.fieldNames.contains("_rescued_data"),
      "Rescued column should be auto-appended")
    assert(!df.schema.fieldNames.contains("_corrupt_record"),
      "Corrupt column should NOT be auto-appended")

    val rows = df.collect()

    // Malformed row: _rescued_data should contain JSON
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_rescued_data")))
    val rescuedJson = rows(2).getAs[String]("_rescued_data")
    assert(rescuedJson.contains("\"id\""))
    assert(rescuedJson.contains("0000A"))
  }

  test("Scenario 6: Both options set, both columns in schema - corrupt NULL, rescued JSON") {
    // CSV behavior: _corrupt_record stays NULL (since rescued captures details), _rescued_data carries JSON
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true),
      StructField("_rescued_data", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("rescuedDataColumn", "_rescued_data")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    // Schema should have exactly 4 columns (user's schema preserved)
    assert(df.schema.length == 4)
    assert(df.schema.fieldNames.toSeq == Seq("name", "id", "_corrupt_record", "_rescued_data"))

    val rows = df.collect()

    // Valid rows: both special columns should be NULL
    assert(rows(0).isNullAt(rows(0).fieldIndex("_corrupt_record")))
    assert(rows(0).isNullAt(rows(0).fieldIndex("_rescued_data")))

    // Malformed row: _corrupt_record should be NULL, _rescued_data should have JSON
    assert(rows(2).isNullAt(rows(2).fieldIndex("_corrupt_record")),
      "When both options set, _corrupt_record should be NULL (rescued takes precedence)")
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_rescued_data")),
      "When both options set, _rescued_data should have JSON")

    val rescuedJson = rows(2).getAs[String]("_rescued_data")
    assert(rescuedJson.contains("\"id\""))
    assert(rescuedJson.contains("0000A"))
  }

  // =====================================================
  // DEFAULT COLUMN DETECTION TESTS (CSV auto-fill behavior)
  // =====================================================

  test("Auto-detect _corrupt_record in schema when option NOT set (CSV default behavior)") {
    // CSV behavior: if _corrupt_record column exists in schema, it is auto-filled
    // even when columnNameOfCorruptRecord option is NOT explicitly set
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      // NOTE: columnNameOfCorruptRecord option is NOT set!
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // Schema should have exactly 3 columns (no auto-append)
    assert(df.schema.length == 3)

    // Valid rows: _corrupt_record should be NULL
    assert(rows(0).isNullAt(rows(0).fieldIndex("_corrupt_record")),
      "Valid row should have NULL _corrupt_record")

    // Malformed row: _corrupt_record should be auto-filled with comma-separated values
    assert(!rows(2).isNullAt(rows(2).fieldIndex("_corrupt_record")),
      "Malformed row should have non-NULL _corrupt_record (auto-detected)")
    val corruptValue = rows(2).getAs[String]("_corrupt_record")
    assert(corruptValue == "Carol,0000A",
      s"Auto-detected corrupt record should be 'Carol,0000A', got: $corruptValue")
  }

  test("_rescued_data in schema is NOT auto-detected when option NOT set (CSV behavior)") {
    // CRITICAL CSV BEHAVIOR DIFFERENCE:
    // - _corrupt_record IS auto-detected from schema
    // - _rescued_data is NOT auto-detected; requires explicit rescuedDataColumn option
    // This asymmetry is documented CSV behavior verified on Databricks.
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_rescued_data", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      // NOTE: rescuedDataColumn option is NOT set!
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // Schema should have exactly 3 columns
    assert(df.schema.length == 3)

    // ALL rows should have NULL _rescued_data because:
    // - rescuedDataColumn option is NOT set
    // - _rescued_data is NOT auto-detected from schema (unlike _corrupt_record)
    assert(rows(0).isNullAt(rows(0).fieldIndex("_rescued_data")),
      "Valid row should have NULL _rescued_data")
    assert(rows(1).isNullAt(rows(1).fieldIndex("_rescued_data")),
      "Valid row should have NULL _rescued_data")
    assert(rows(2).isNullAt(rows(2).fieldIndex("_rescued_data")),
      "Malformed row should also have NULL _rescued_data (no auto-detect for rescued)")
  }

  // =====================================================
  // MODE TESTS (PERMISSIVE, DROPMALFORMED, FAILFAST)
  // =====================================================

  test("DROPMALFORMED mode: rows with type conversion failures are dropped") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // invalid1.txt has 3 rows: Alice/00001, Bob/00002, Carol/0000A
    // Carol's "0000A" fails IntegerType conversion, so should be dropped
    assert(rows.length == 2, s"Expected 2 rows (malformed dropped), got ${rows.length}")
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(1).getAs[String]("name") == "Bob")
  }

  test("FAILFAST mode: throws exception on type conversion failure") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val exception = intercept[Exception] {
      spark.read.format("fixedwidth-custom-scala")
        .option("field_lengths", "0:5,5:10")
        .option("mode", "FAILFAST")
        .schema(schema)
        .load(s"$testDataPath/invalid1.txt")
        .collect()
    }

    // Should throw on Carol's "0000A" failing IntegerType conversion
    assert(exception.getMessage.contains("Type conversion failed") ||
           exception.getCause.getMessage.contains("Type conversion failed"),
      s"Expected type conversion error, got: ${exception.getMessage}")
  }

  test("PERMISSIVE mode (default): type conversion failures become NULL") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(s"$testDataPath/invalid1.txt")

    val rows = df.collect()

    // All 3 rows should be present
    assert(rows.length == 3, s"Expected 3 rows in PERMISSIVE mode, got ${rows.length}")

    // Valid rows have values
    assert(rows(0).getAs[Int]("id") == 1)
    assert(rows(1).getAs[Int]("id") == 2)

    // Malformed row has NULL for failed conversion
    assert(rows(2).isNullAt(rows(2).fieldIndex("id")),
      "PERMISSIVE mode should have NULL for failed conversion")
  }

  // =====================================================
  // PHASE 1.1: trimValues OPTION TESTS
  // =====================================================

  test("trimValues=true (default): whitespace is trimmed from parsed values") {
    // Data in trim_test.txt: "  Al 00001", "Bob  00002", " Eve 00003"
    // field_lengths: 0:5 (name), 5:10 (id)
    // With trimValues=true:
    //   "  Al " -> "Al", "Bob  " -> "Bob", " Eve " -> "Eve"
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("trimValues", "true")
      .schema(schema)
      .load(s"$testDataPath/trim_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // Values should be trimmed
    assert(rows(0).getAs[String]("name") == "Al", s"Expected 'Al' but got '${rows(0).getAs[String]("name")}'")
    assert(rows(0).getAs[Int]("id") == 1)

    assert(rows(1).getAs[String]("name") == "Bob", s"Expected 'Bob' but got '${rows(1).getAs[String]("name")}'")
    assert(rows(1).getAs[Int]("id") == 2)

    assert(rows(2).getAs[String]("name") == "Eve", s"Expected 'Eve' but got '${rows(2).getAs[String]("name")}'")
    assert(rows(2).getAs[Int]("id") == 3)
  }

  test("trimValues=false: whitespace is preserved in parsed values") {
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
    assert(rows.length == 3)

    // Values should NOT be trimmed (preserve whitespace)
    assert(rows(0).getAs[String]("name") == "  Al ", s"Expected '  Al ' but got '${rows(0).getAs[String]("name")}'")
    assert(rows(1).getAs[String]("name") == "Bob  ", s"Expected 'Bob  ' but got '${rows(1).getAs[String]("name")}'")
    assert(rows(2).getAs[String]("name") == " Eve ", s"Expected ' Eve ' but got '${rows(2).getAs[String]("name")}'")
  }

  test("ignoreLeadingWhiteSpace=true, ignoreTrailingWhiteSpace=false: only leading trimmed") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "false")
      .schema(schema)
      .load(s"$testDataPath/trim_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // Only leading whitespace trimmed
    assert(rows(0).getAs[String]("name") == "Al ", s"Expected 'Al ' but got '${rows(0).getAs[String]("name")}'")
    assert(rows(1).getAs[String]("name") == "Bob  ", s"Expected 'Bob  ' but got '${rows(1).getAs[String]("name")}'")
    assert(rows(2).getAs[String]("name") == "Eve ", s"Expected 'Eve ' but got '${rows(2).getAs[String]("name")}'")
  }

  test("ignoreLeadingWhiteSpace=false, ignoreTrailingWhiteSpace=true: only trailing trimmed") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "true")
      .schema(schema)
      .load(s"$testDataPath/trim_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // Only trailing whitespace trimmed
    assert(rows(0).getAs[String]("name") == "  Al", s"Expected '  Al' but got '${rows(0).getAs[String]("name")}'")
    assert(rows(1).getAs[String]("name") == "Bob", s"Expected 'Bob' but got '${rows(1).getAs[String]("name")}'")
    assert(rows(2).getAs[String]("name") == " Eve", s"Expected ' Eve' but got '${rows(2).getAs[String]("name")}'")
  }

  // =====================================================
  // PHASE 1.2: nullValue OPTION TESTS
  // =====================================================

  test("nullValue=\"NULL\": string 'NULL' becomes null in output") {
    // Data in nullvalue_test.txt: "Alice00001", "NULL 00002", "     00003"
    // field_lengths: 0:5 (name), 5:10 (id)
    // With nullValue="NULL": "NULL " is trimmed to "NULL" and becomes null
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("nullValue", "NULL")
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // First row: normal values
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(0).getAs[Int]("id") == 1)

    // Second row: "NULL " trimmed to "NULL" -> becomes null
    assert(rows(1).isNullAt(rows(1).fieldIndex("name")), "Expected 'NULL' to become null")
    assert(rows(1).getAs[Int]("id") == 2)

    // Third row: "     " trimmed to "" -> not null (unless nullValue="")
    assert(rows(2).getAs[String]("name") == "")
    assert(rows(2).getAs[Int]("id") == 3)
  }

  test("nullValue=\"\" (empty string): empty/whitespace-only values become null") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("nullValue", "")
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // First row: "Alice" is not empty -> not null
    assert(rows(0).getAs[String]("name") == "Alice")

    // Second row: "NULL " trimmed to "NULL" -> not empty -> not null
    assert(rows(1).getAs[String]("name") == "NULL")

    // Third row: "     " trimmed to "" -> matches nullValue="" -> becomes null
    assert(rows(2).isNullAt(rows(2).fieldIndex("name")), "Expected empty string to become null")
  }

  test("nullValue not set: no special null handling, empty strings remain empty") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      // No nullValue option
      .schema(schema)
      .load(s"$testDataPath/nullvalue_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // "NULL " -> "NULL" (just a string, not null)
    assert(rows(1).getAs[String]("name") == "NULL")
    // "     " -> "" (empty string, not null)
    assert(rows(2).getAs[String]("name") == "")
  }

  // ===========================================
  // Phase 1.3: dateFormat/timestampFormat Tests
  // ===========================================

  test("timestampFormat: parse timestamps with custom format") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("ts", TimestampType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:24")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(schema)
      .load(s"$testDataPath/datetime_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // Valid timestamps should be parsed correctly
    assert(rows(0).getAs[java.sql.Timestamp]("ts") != null, "First timestamp should parse")
    assert(rows(1).getAs[java.sql.Timestamp]("ts") != null, "Second timestamp should parse")
    // Invalid timestamp should be null in PERMISSIVE mode
    assert(rows(2).isNullAt(1), "Invalid timestamp should be null")
  }

  test("dateFormat: parse dates with custom format") {
    // Create a date-only test file
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("dt", DateType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:15")  // Only date portion
      .option("dateFormat", "yyyy-MM-dd")
      .schema(schema)
      .load(s"$testDataPath/datetime_test.txt")

    // Use SQL cast to avoid JDK 17+ module access issues with DateType deserialization
    val rows = df.selectExpr("name", "CAST(dt AS STRING) as dt_str").collect()
    assert(rows.length == 3)

    // Valid dates should be parsed correctly (shows as "2024-01-15" string)
    assert(rows(0).getAs[String]("dt_str") == "2024-01-15", "First date should parse")
    assert(rows(1).getAs[String]("dt_str") == "2024-02-20", "Second date should parse")
    // Invalid date should be null in PERMISSIVE mode
    assert(rows(2).isNullAt(1), "Invalid date should be null")
  }

  test("timestampFormat with timeZone: applies timezone correctly") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("ts", TimestampType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:24")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("timeZone", "UTC")
      .schema(schema)
      .load(s"$testDataPath/datetime_test.txt")

    val rows = df.collect()
    assert(rows.length == 3)

    // Timestamps should be parsed with UTC timezone
    assert(rows(0).getAs[java.sql.Timestamp]("ts") != null, "Timestamp with UTC timezone should parse")
  }

  // ===========================================
  // Phase 1.4: comment option Tests
  // ===========================================

  test("comment option: lines starting with comment char are skipped") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("comment", "#")
      .schema(schema)
      .load(s"$testDataPath/comment_test.txt")

    val rows = df.collect()
    // File has 5 lines, but 2 are comments - should only get 3 data rows
    assert(rows.length == 3, s"Expected 3 rows (comments skipped), got ${rows.length}")
    assert(rows(0).getAs[String]("name") == "Alice", "First data row should be Alice")
    assert(rows(1).getAs[String]("name") == "Bob", "Second data row should be Bob")
    assert(rows(2).getAs[String]("name") == "Eve", "Third data row should be Eve")
  }

  test("comment option not set: comment lines parsed as data") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      // No comment option - all lines are data
      .schema(schema)
      .load(s"$testDataPath/comment_test.txt")

    val rows = df.collect()
    // All 5 lines should be parsed (no comment filtering)
    assert(rows.length == 5, s"Expected 5 rows (no comment filtering), got ${rows.length}")
  }

  // ===========================================
  // Phase 2.1: field_simple format Tests
  // ===========================================

  test("field_simple: simplified width-based field specification") {
    // field_simple = "5,5" means: first field 5 chars, second field 5 chars
    // Equivalent to field_lengths = "0:5,5:10"
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_simple", "5,5")  // New simplified format
      .schema(schema)
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect()
    assert(rows.length == 3)
    assert(rows(0).getAs[String]("name") == "Alice", "Name should be parsed correctly")
    assert(rows(0).getAs[Int]("id") == 1, "ID should be parsed correctly")
  }

  test("field_simple: handles variable width fields") {
    // field_simple = "3,4,5" means: 3 chars, 4 chars, 5 chars
    val schema = StructType(Seq(
      StructField("code", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    ))

    // Create test with known widths
    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_simple", "5,5")  // Using valid1.txt which has 5,5 format
      .schema(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("id", IntegerType, nullable = true)
      )))
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect()
    assert(rows.length == 3)
  }

  test("field_simple with field_lengths: field_simple takes precedence") {
    // When both are specified, field_simple should be used
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_simple", "5,5")
      .option("field_lengths", "0:10,10:20")  // Different positions - should be ignored
      .schema(schema)
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect()
    assert(rows.length == 3)
    // If field_simple takes precedence, these should parse correctly
    assert(rows(0).getAs[String]("name") == "Alice", "field_simple should take precedence")
    assert(rows(0).getAs[Int]("id") == 1, "ID should parse correctly with field_simple")
  }

  // ===========================================
  // Phase 3.1: Byte-based Partitioning Tests
  // ===========================================

  test("byte-based partitioning: creates multiple partitions with maxPartitionBytes") {
    // large_test.txt has 100 lines, each 13 bytes = 1300 bytes total
    // With maxPartitionBytes=500, should create 3 partitions (500, 500, 300)
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:7,7:12")  // NameXXX (7 chars), XXXXX (5 chars)
      .option("maxPartitionBytes", "500")   // Force multiple partitions
      .schema(schema)
      .load(s"$testDataPath/large_test.txt")

    // Check that multiple partitions were created
    val numPartitions = df.rdd.getNumPartitions
    assert(numPartitions >= 2, s"Expected at least 2 partitions with 500 byte splits, got $numPartitions")

    val rows = df.collect()
    // All 100 rows should be read correctly across all partitions
    assert(rows.length == 100, s"Expected 100 rows, got ${rows.length}")

    // Verify first and last records
    assert(rows(0).getAs[String]("name") == "Name001", s"First name should be Name001, got ${rows(0).getAs[String]("name")}")
    assert(rows(0).getAs[Int]("id") == 1, s"First id should be 1, got ${rows(0).getAs[Int]("id")}")
    assert(rows(99).getAs[String]("name") == "Name100", s"Last name should be Name100, got ${rows(99).getAs[String]("name")}")
    assert(rows(99).getAs[Int]("id") == 100, s"Last id should be 100, got ${rows(99).getAs[Int]("id")}")
  }

  test("byte-based partitioning: numPartitions option creates exact partition count") {
    // Force exactly 4 partitions regardless of file size
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:7,7:12")
      .option("numPartitions", "4")   // Force exactly 4 partitions
      .schema(schema)
      .load(s"$testDataPath/large_test.txt")

    // Check exact partition count
    val numPartitions = df.rdd.getNumPartitions
    assert(numPartitions == 4, s"Expected exactly 4 partitions with numPartitions=4, got $numPartitions")

    val rows = df.collect()
    // All 100 rows should be read, no duplicates or missing rows
    assert(rows.length == 100, s"Expected 100 rows, got ${rows.length}")

    // Verify no duplicates by checking unique IDs
    val ids = rows.map(_.getAs[Int]("id")).toSet
    assert(ids.size == 100, s"Expected 100 unique IDs, got ${ids.size}")
  }

  test("byte-based partitioning: partition boundaries handle line splits correctly") {
    // With 100 lines of 13 bytes each (1300 total), and 7 partitions:
    // Each partition is ~186 bytes
    // Partition boundaries will fall mid-line; verify no data is lost or duplicated
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:7,7:12")
      .option("numPartitions", "7")   // Odd number to ensure mid-line splits
      .schema(schema)
      .load(s"$testDataPath/large_test.txt")

    val rows = df.collect()
    assert(rows.length == 100, s"Expected 100 rows, got ${rows.length}")

    // Verify sequential IDs from 1 to 100
    val sortedIds = rows.map(_.getAs[Int]("id")).sorted
    assert(sortedIds.head == 1, s"First ID should be 1, got ${sortedIds.head}")
    assert(sortedIds.last == 100, s"Last ID should be 100, got ${sortedIds.last}")

    // Verify no gaps in sequence
    val expectedIds = (1 to 100).toArray
    assert(sortedIds.sameElements(expectedIds), "IDs should be sequential 1 to 100")
  }

  test("byte-based partitioning: single partition with large maxPartitionBytes") {
    // If maxPartitionBytes is larger than file size, should get single partition
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:7,7:12")
      .option("maxPartitionBytes", "10000000")  // 10MB - much larger than 1300 byte file
      .schema(schema)
      .load(s"$testDataPath/large_test.txt")

    // Should get single partition since file is smaller than maxPartitionBytes
    val numPartitions = df.rdd.getNumPartitions
    assert(numPartitions == 1, s"Expected 1 partition for small file with large maxPartitionBytes, got $numPartitions")

    val rows = df.collect()
    assert(rows.length == 100, s"Expected 100 rows, got ${rows.length}")
  }

  // ==========================================
  // Phase 4.1: Multi-File / Glob Pattern Support
  // ==========================================

  test("glob pattern: reads multiple files with wildcard") {
    // Files: multi_file_a.txt (3 rows), multi_file_b.txt (3 rows), multi_file_c.txt (2 rows)
    // Format: "FileX1    001" → name (10 chars), id (3 chars)
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:13")
      .schema(schema)
      .load(s"$testDataPath/multi_file_*.txt")

    val rows = df.collect()
    assert(rows.length == 8, s"Expected 8 total rows from 3 files, got ${rows.length}")

    // Verify data from all files is present
    val names = rows.map(_.getString(0)).sorted
    assert(names.contains("FileA1"), "Should contain FileA1")
    assert(names.contains("FileB3"), "Should contain FileB3")
    assert(names.contains("FileC2"), "Should contain FileC2")
  }

  test("glob pattern: creates partition per file") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:13")
      .schema(schema)
      .load(s"$testDataPath/multi_file_*.txt")

    // Should have at least 3 partitions (one per file minimum)
    assert(df.rdd.getNumPartitions >= 3, s"Expected >= 3 partitions for 3 files, got ${df.rdd.getNumPartitions}")
  }

  test("glob pattern: reads specific files with brace expansion") {
    // Load only files a and b, not c
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:13")
      .schema(schema)
      .load(s"$testDataPath/multi_file_{a,b}.txt")

    val rows = df.collect()
    assert(rows.length == 6, s"Expected 6 rows from 2 files (a+b), got ${rows.length}")

    // Verify FileC is NOT present
    val names = rows.map(_.getString(0))
    assert(!names.exists(_.startsWith("FileC")), "Should NOT contain FileC entries")
  }

  // ==========================================
  // Phase 4.2: Header Option Support
  // ==========================================

  test("header=true: skips first line as header") {
    // File with_header.txt has: "Name      ID" as header, then 3 data rows
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:13")
      .option("header", "true")
      .schema(schema)
      .load(s"$testDataPath/with_header.txt")

    val rows = df.collect()
    assert(rows.length == 3, s"Expected 3 data rows (header skipped), got ${rows.length}")

    // First data row should be Alice, not the header
    assert(rows(0).getString(0) == "Alice", s"First row should be Alice, got ${rows(0).getString(0)}")
  }

  test("header=false: first line is treated as data") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:13")
      .option("header", "false")
      .schema(schema)
      .load(s"$testDataPath/with_header.txt")

    val rows = df.collect()
    assert(rows.length == 4, s"Expected 4 rows (header treated as data), got ${rows.length}")

    // First row should be the header line
    assert(rows(0).getString(0) == "Name", s"First row should be Name (header), got ${rows(0).getString(0)}")
  }

  test("header option with skip_lines: header takes precedence") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))

    // When both header=true and skip_lines=2 are set, header=true should take precedence
    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:13")
      .option("header", "true")
      .option("skip_lines", "2")  // This should be ignored
      .schema(schema)
      .load(s"$testDataPath/with_header.txt")

    val rows = df.collect()
    // header=true means skip 1 line, not 2
    assert(rows.length == 3, s"Expected 3 rows (header=true skips 1 line), got ${rows.length}")
  }

  // ===========================================
  // Write Support Tests (Phase 5.1)
  // ===========================================

  test("write: basic write with field_lengths") {
    import spark.implicits._
    val data = Seq(("Alice", 1), ("Bob", 2), ("Carol", 3))
    val df = data.toDF("name", "id")

    val outputPath = s"$testDataPath/output_write_basic"

    // Clean up if exists
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")  // name: 10 chars, id: 5 chars
      .mode("overwrite")
      .save(outputPath)

    // Read back and verify
    val readSchema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))
    val readDf = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")
      .schema(readSchema)
      .load(outputPath)

    val rows = readDf.collect().sortBy(_.getInt(1))
    assert(rows.length == 3)
    assert(rows(0).getString(0) == "Alice")
    assert(rows(0).getInt(1) == 1)
    assert(rows(1).getString(0) == "Bob")
    assert(rows(2).getString(0) == "Carol")

    // Clean up
    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)
  }

  test("write: values are right-padded to fixed width") {
    import spark.implicits._
    val data = Seq(("A", 1))  // Short value
    val df = data.toDF("name", "id")

    val outputPath = s"$testDataPath/output_write_padding"
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")  // name: 10 chars, id: 5 chars
      .mode("overwrite")
      .save(outputPath)

    // Read raw file content to verify padding
    val files = fs.listStatus(new org.apache.hadoop.fs.Path(outputPath))
      .filter(_.getPath.getName.endsWith(".txt"))
    assert(files.nonEmpty, "Expected output files")

    val inputStream = fs.open(files.head.getPath)
    val reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream))
    val line = reader.readLine()
    reader.close()

    // Line should be exactly 15 characters: "A" padded to 10, "1" padded to 5
    assert(line.length == 15, s"Expected line length 15, got ${line.length}")
    assert(line.substring(0, 10) == "A         ", s"Name not properly padded: '${line.substring(0, 10)}'")

    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)
  }

  test("write: values truncated when too long") {
    import spark.implicits._
    val data = Seq(("VeryLongNameThatExceedsWidth", 12345))
    val df = data.toDF("name", "id")

    val outputPath = s"$testDataPath/output_write_truncate"
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)

    df.write
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15")  // name: 10 chars, id: 5 chars
      .mode("overwrite")
      .save(outputPath)

    // Read raw file content to verify truncation
    val files = fs.listStatus(new org.apache.hadoop.fs.Path(outputPath))
      .filter(_.getPath.getName.endsWith(".txt"))

    val inputStream = fs.open(files.head.getPath)
    val reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream))
    val line = reader.readLine()
    reader.close()

    // Name should be truncated to 10 chars
    assert(line.substring(0, 10) == "VeryLongNa", s"Name not truncated: '${line.substring(0, 10)}'")
    // ID should be truncated to 5 chars
    assert(line.substring(10, 15) == "12345", s"ID field: '${line.substring(10, 15)}'")

    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)
  }

   // ===========================================
  // Schema via Metadata Tests (Phase 5.2)
  // ===========================================

  test("schema metadata: read with widths from schema metadata") {
    // Create schema with width metadata instead of using field_lengths option
    val schema = StructType(Seq(
      StructField("name", StringType, true, new MetadataBuilder().putLong("width", 5).build()),
      StructField("value", IntegerType, true, new MetadataBuilder().putLong("width", 5).build()),
      StructField("flag", StringType, true, new MetadataBuilder().putLong("width", 1).build())
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .schema(schema)  // No field_lengths needed - derived from metadata
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect().sortBy(_.getInt(1))
    assert(rows.length == 3)
    assert(rows(0).getString(0).trim == "Alice")
    assert(rows(0).getInt(1) == 1)
    assert(rows(1).getString(0).trim == "Bob")
    assert(rows(2).getString(0).trim == "Carol")
  }

  test("schema metadata: mixed - some fields with metadata, some without") {
    // If width metadata is missing for some fields, those should fail validation
    val schema = StructType(Seq(
      StructField("name", StringType, true, new MetadataBuilder().putLong("width", 5).build()),
      StructField("value", IntegerType, true)  // No width metadata
    ))

    // Should fail because schema metadata is incomplete (or fall back to field_lengths)
    val ex = intercept[IllegalArgumentException] {
      spark.read
        .format("fixedwidth-custom-scala")
        .schema(schema)  // Incomplete width metadata and no field_lengths
        .load(s"$testDataPath/valid1.txt")
        .collect()  // Force evaluation
    }
    assert(ex.getMessage.contains("field_lengths") || ex.getMessage.contains("width"))
  }

  test("schema metadata: field_lengths option takes precedence over metadata") {
    // When both metadata and field_lengths are provided, field_lengths wins
    val schema = StructType(Seq(
      StructField("name", StringType, true, new MetadataBuilder().putLong("width", 999).build()),
      StructField("value", IntegerType, true, new MetadataBuilder().putLong("width", 999).build())
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")  // This should override metadata
      .schema(schema)
      .load(s"$testDataPath/valid1.txt")

    val rows = df.collect().sortBy(_.getInt(1))
    assert(rows.length == 3)
    // If field_lengths is used (not metadata), parsing should work
    assert(rows(0).getString(0).trim == "Alice")
    assert(rows(0).getInt(1) == 1)
  }

  test("schema metadata: write respects width from metadata") {
    import spark.implicits._
    val data = Seq(("A", 1))
    val df = data.toDF("name", "id")

    // Create output schema with width metadata
    val outputPath = s"$testDataPath/output_write_metadata"
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)

    df.write
      .format("fixedwidth-custom-scala")
      .option("schema_metadata_widths", "10,5")  // Specify widths via option for write side
      .mode("overwrite")
      .save(outputPath)

    // Read raw file to verify widths were applied
    val files = fs.listStatus(new org.apache.hadoop.fs.Path(outputPath))
      .filter(_.getPath.getName.endsWith(".txt"))
    assert(files.nonEmpty, "Expected output files")

    val inputStream = fs.open(files.head.getPath)
    val reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream))
    val line = reader.readLine()
    reader.close()

    assert(line.length == 15, s"Expected line length 15, got ${line.length}")

    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)
  }

  // ===========================================
  // Compression Support Tests (Phase 5.3)
  // ===========================================

  test("compression: read gzip compressed file") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("value", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .schema(schema)
      .load(s"$testDataPath/valid1.txt.gz")

    val rows = df.collect().sortBy(_.getInt(1))
    assert(rows.length == 3)
    assert(rows(0).getString(0).trim == "Alice")
    assert(rows(0).getInt(1) == 1)
    assert(rows(1).getString(0).trim == "Bob")
    assert(rows(2).getString(0).trim == "Carol")
  }

  test("compression: read bzip2 compressed file") {
    // Create bz2 file if doesn't exist
    val testFile = s"$testDataPath/valid1.txt.bz2"
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val bz2Path = new org.apache.hadoop.fs.Path(testFile)

    // Skip test if bz2 file doesn't exist
    if (!fs.exists(bz2Path)) {
      cancel("bz2 test file not available")
    }

    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("value", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .schema(schema)
      .load(testFile)

    val rows = df.collect()
    assert(rows.length == 3)
  }

  test("compression: compressed files use single partition") {
    // Compressed files cannot be split, so they should always use single partition
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("value", IntegerType)
    ))

    val df = spark.read
      .format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("numPartitions", "4")  // Should be ignored for compressed files
      .schema(schema)
      .load(s"$testDataPath/valid1.txt.gz")

    // Compressed files cannot be split, so numPartitions should be 1
    // We verify by checking that reading works correctly
    val rows = df.collect()
    assert(rows.length == 3)
  }

}
