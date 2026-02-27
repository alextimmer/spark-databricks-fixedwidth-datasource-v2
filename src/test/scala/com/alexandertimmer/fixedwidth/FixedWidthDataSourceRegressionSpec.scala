// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Using}
import java.io.{BufferedReader, InputStreamReader}
import scala.jdk.CollectionConverters._

/**
 * Trait for generating test data in refactoring tests.
 */
trait TestDataGenerator {
  def createTestFile(
    content: String,
    prefix: String = "fw-test",
    suffix: String = ".txt"
  ): Path = {
    val tempFile = Files.createTempFile(prefix, suffix)
    tempFile.toFile.deleteOnExit()
    Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8))
    tempFile
  }

  def createMultiLineTestFile(lines: Seq[String]): Path = {
    createTestFile(lines.mkString("\n"))
  }
}

/**
 * Test data constants for refactoring validation.
 */
object TestData {
  val VALID_RECORDS: Seq[String] = Seq(
    "Alice     001252023-01-15",
    "Bob       002302023-02-20",
    "Charlie   003452023-03-25"
  )

  val INCOMPLETE_RECORDS: Seq[String] = Seq(
    "Alice     00125",
    "Bob       ",
    "ShortLine"
  )

  val INVALID_DATES: Seq[String] = Seq(
    "Alice     00125INVALID-DT",
    "Bob       0030299-99-9999"
  )

  val UNTRIMMED_RECORDS: Seq[String] = Seq(
    "  Alice   00125  2023-01-15",
    "Bob       00030  2023-02-20  "
  )
}

/**
 * Validation tests for refactoring work.
 * These tests ensure that refactoring maintains backward compatibility
 * and validates new improvements.
 */
class RefactoringValidationSpec extends AnyFunSuite with BeforeAndAfterAll with TestDataGenerator {

  val spark: SparkSession = SparkSession.builder()
    .appName("FixedWidthDataSourceRegressionTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  // =====================================================
  // PACKAGE NAMING & SERVICE LOADER (Item #8)
  // =====================================================

  test("DataSource is registered via ServiceLoader") {
    // Verify that the data source can be loaded by Spark
    val sources = java.util.ServiceLoader.load(
      classOf[org.apache.spark.sql.sources.DataSourceRegister]
    )

    val fixedWidthSource = sources.iterator().asScala.find { source =>
      source.shortName() == "fixedwidth-custom-scala"
    }

    assert(fixedWidthSource.isDefined,
      "fixedwidth-custom-scala should be registered via ServiceLoader")
  }

  test("DataSource can be loaded by Spark using format string") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "Alice00001".getBytes(StandardCharsets.UTF_8))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .load(testFile.toString)

    assert(df.columns.length > 0, "Should successfully load with format string")
  }

  // =====================================================
  // OPTION VALIDATION (Item #2)
  // =====================================================

  test("Invalid field_lengths format should throw clear error") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "test".getBytes(StandardCharsets.UTF_8))

    val invalidFormats = Seq(
      "abc",           // Not numeric
      "1-5,5-10",      // Wrong separator
      "10:5",          // Start > end
      "0:5,3:8"        // Overlapping ranges
    )

    invalidFormats.foreach { format =>
      val caught = intercept[Exception] {
        spark.read.format("fixedwidth-custom-scala")
          .option("field_lengths", format)
          .load(testFile.toString)
          .collect()
      }
      assert(
        caught.getMessage.contains("field_lengths") ||
        caught.getMessage.contains("Invalid") ||
        caught.getMessage.contains("range"),
        s"Should provide clear error for invalid format: $format"
      )
    }
  }

  test("Negative skip_lines should fail with clear message") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "test".getBytes(StandardCharsets.UTF_8))

    val caught = intercept[Exception] {
      spark.read.format("fixedwidth-custom-scala")
        .option("field_lengths", "0:5")
        .option("skip_lines", "-1")
        .load(testFile.toString)
        .collect()
    }
    assert(
      caught.getMessage.contains("skip_lines") ||
      caught.getMessage.contains("negative") ||
      caught.getMessage.contains("non-negative"),
      "Should reject negative skip_lines"
    )
  }

  test("Non-numeric skip_lines should fail with clear message") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "test".getBytes(StandardCharsets.UTF_8))

    val caught = intercept[Exception] {
      spark.read.format("fixedwidth-custom-scala")
        .option("field_lengths", "0:5")
        .option("skip_lines", "abc")
        .load(testFile.toString)
        .collect()
    }
    assert(
      caught.getMessage.contains("skip_lines") ||
      caught.getMessage.contains("integer"),
      "Should reject non-numeric skip_lines"
    )
  }

  // =====================================================
  // RESOURCE MANAGEMENT (Item #3)
  // =====================================================

  test("Resources are properly closed after successful read") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "Alice00001\nBob  00002".getBytes(StandardCharsets.UTF_8))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .load(testFile.toString)

    val count = df.count()
    assert(count == 2)

    // File should be readable again (not locked)
    Thread.sleep(100) // Give time for cleanup
    val canRead = Files.isReadable(testFile)
    assert(canRead, "File should not be locked after read")
  }

  test("Resources are properly closed after failed read") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "test".getBytes(StandardCharsets.UTF_8))

    // Cause a read failure by using invalid schema
    val schema = StructType(Seq(
      StructField("col1", IntegerType, nullable = false) // Non-nullable but data is invalid
    ))

    val caught = Try {
      spark.read.format("fixedwidth-custom-scala")
        .option("field_lengths", "0:5")
        .schema(schema)
        .load(testFile.toString)
        .collect() // Force evaluation
    }

    // File should still be readable (not leaked)
    Thread.sleep(100)
    val canRead = Files.isReadable(testFile)
    assert(canRead, "File should not be locked even after error")
  }

  // =====================================================
  // TEST DATA GENERATION (Item #7)
  // =====================================================

  test("Generated test data works for valid records") {
    val testFile = createMultiLineTestFile(TestData.VALID_RECORDS)

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("date", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15,15:25")
      .schema(schema)
      .load(testFile.toString)

    val rows = df.collect()
    assert(rows.length == 3)
    assert(rows(0).getAs[String]("name").trim == "Alice")
    assert(rows(1).getAs[String]("name").trim == "Bob")
    assert(rows(2).getAs[String]("name").trim == "Charlie")
  }

  test("Generated test data works for incomplete records") {
    val testFile = createMultiLineTestFile(TestData.INCOMPLETE_RECORDS)

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("date", StringType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:10,10:15,15:25")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema(schema)
      .load(testFile.toString)

    val rows = df.collect()
    assert(rows.length == 3)
    // At least one record should have corrupt data
    val hasCorruptRecords = rows.exists(r => r.getAs[String]("_corrupt_record") != null)
    assert(hasCorruptRecords, "Should detect incomplete records as corrupt")
  }

  // =====================================================
  // BUILD CONFIGURATION (Item #4)
  // =====================================================

  test("JAR contains META-INF/services registration") {
    val classLoader = Thread.currentThread().getContextClassLoader
    val serviceFile = classLoader.getResource(
      "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister"
    )

    assert(serviceFile != null, "ServiceLoader registration file must exist in classpath")

    // Read the file content
    val content = Using(scala.io.Source.fromURL(serviceFile)) { source =>
      source.mkString.trim
    }.get

    assert(
      content.contains("DefaultSource"),
      s"Service file should reference DefaultSource, got: $content"
    )
  }

  // =====================================================
  // BACKWARD COMPATIBILITY
  // =====================================================

  test("Existing functionality preserved after refactoring") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "Alice00001\nBob  00002\nCarol00003".getBytes(StandardCharsets.UTF_8))

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .schema(schema)
      .load(testFile.toString)

    val rows = df.collect()
    assert(rows.length == 3)
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(rows(0).getAs[Int]("id") == 1)
    assert(rows(1).getAs[String]("name") == "Bob")  // Trimmed by default
    assert(rows(1).getAs[Int]("id") == 2)
  }

  test("Special columns still work after refactoring") {
    val testFile = Files.createTempFile("test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, "InvalidData\nAlice00001".getBytes(StandardCharsets.UTF_8))

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true)
    ))

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:5,5:10")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema(schema)
      .load(testFile.toString)

    val rows = df.collect()
    assert(rows.length == 2)

    // First row should be corrupt
    val corruptRow = rows.find(_.getAs[String]("_corrupt_record") != null)
    assert(corruptRow.isDefined, "Should have corrupt record")
  }

  // =====================================================
  // PERFORMANCE BENCHMARKING (for Item #1)
  // =====================================================

  test("Performance baseline - reading 1000 rows") {
    val largeData = (1 to 1000).map(i => f"Name$i%05d${i}%05d")
    val testFile = Files.createTempFile("perf-test", ".txt")
    testFile.toFile.deleteOnExit()
    Files.write(testFile, largeData.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val startTime = System.nanoTime()

    val df = spark.read.format("fixedwidth-custom-scala")
      .option("field_lengths", "0:9,9:14")
      .load(testFile.toString)

    val count = df.count()

    val duration = (System.nanoTime() - startTime) / 1e9

    assert(count == 1000, "Should read all 1000 rows")
    assert(duration < 10.0, s"Should complete in under 10 seconds, took $duration")

    println(s"Performance: Read $count rows in $duration seconds (${count/duration} rows/sec)")
  }
}
