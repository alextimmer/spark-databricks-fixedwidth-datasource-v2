// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path => JPath}

/**
 * Split-boundary correctness for CR/LF/CRLF files, the read-side `lineSep`
 * option, and splittable (bz2) compressed input.
 *
 * Background: before 0.2.1 the reader tracked its position with
 * `line.getBytes(encoding).length + 1`, undercounting every CRLF line by one
 * byte. A split then overshot its end and the next split re-read those lines
 * (106,522 duplicated rows on a 154 MB production file). Fixtures are generated
 * at runtime: 100 rows, 7-char name + 5-char zero-padded id, field_lengths "0:7,7:12".
 */
class LineEndingSplitSpec extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("LineEndingSplitTest")
    .master("local[*]")
    .getOrCreate()

  private val FieldLengths = "0:7,7:12"

  private val nameIdSchema = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("id", IntegerType, nullable = true)
  ))

  private val withCorruptSchema = StructType(nameIdSchema.fields :+
    StructField("_corrupt_record", StringType, nullable = true))

  /** 100 rows, 12 bytes each, no terminator: "Name001" + "00001". */
  private val rows100: Seq[String] = (1 to 100).map(i => f"Name$i%03d$i%05d")

  private def joined(lines: Seq[String], sep: String, trailing: Boolean = true): String =
    lines.mkString(sep) + (if (trailing) sep else "")

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
    val dir = Files.createTempDirectory("fw-lineending-spec")
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

  private def writeFile(dir: JPath, name: String, content: String,
                        charset: Charset = StandardCharsets.UTF_8): JPath =
    Files.write(dir.resolve(name), content.getBytes(charset))

  /** Exactly 100 rows, ids == 1..100 as a set AND as a sorted list, no null ids, no corrupt rows. */
  private def assertExactly100(df: DataFrame): Unit = {
    val rows = df.collect()
    assert(rows.length == 100, s"Expected 100 rows, got ${rows.length}")

    val nullIds = rows.filter(r => r.isNullAt(r.fieldIndex("id")))
    assert(nullIds.isEmpty,
      s"${nullIds.length} rows have a null id (mis-aligned split?): ${nullIds.take(3).map(_.toSeq).toSeq}")

    val ids = rows.map(_.getAs[Int]("id"))
    assert(ids.toSet == (1 to 100).toSet,
      s"missing=${(1 to 100).toSet.diff(ids.toSet).toSeq.sorted} unexpected=${ids.toSet.diff((1 to 100).toSet).toSeq.sorted}")
    val dupes = ids.groupBy(identity).collect { case (k, v) if v.length > 1 => k }.toSeq.sorted
    assert(ids.sorted.toSeq == (1 to 100), s"Duplicate ids across splits: $dupes")

    if (df.schema.fieldNames.contains("_corrupt_record")) {
      val corrupt = rows.filterNot(r => r.isNullAt(r.fieldIndex("_corrupt_record")))
      assert(corrupt.isEmpty,
        s"${corrupt.length} corrupt rows: ${corrupt.take(3).map(_.getAs[String]("_corrupt_record")).toSeq}")
    }
  }

  private def causeChain(t: Throwable): Iterator[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)

  // a
  test("CRLF file split by maxPartitionBytes=500 yields exactly 100 unique rows") {
    withTempDir { dir =>
      val file = writeFile(dir, "crlf.txt", joined(rows100, "\r\n"))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("maxPartitionBytes" -> "500"))(file.toString)
      assert(df.rdd.getNumPartitions >= 2,
        s"1400-byte CRLF file at maxPartitionBytes=500 must split, got ${df.rdd.getNumPartitions}")
      assertExactly100(df)
    }
  }

  // b
  test("CRLF file with numPartitions=7 yields exactly 7 partitions and 100 unique rows") {
    withTempDir { dir =>
      val file = writeFile(dir, "crlf.txt", joined(rows100, "\r\n"))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("numPartitions" -> "7"))(file.toString)
      assert(df.rdd.getNumPartitions == 7, s"Expected 7 partitions, got ${df.rdd.getNumPartitions}")
      assertExactly100(df)
    }
  }

  // c
  test("CRLF file split at every offset mod 14 (maxPartitionBytes=13) yields 100 unique rows") {
    // 14-byte lines and 13-byte splits: boundaries fall on every byte position of a
    // line over the file, including exactly between '\r' and '\n'.
    withTempDir { dir =>
      val file = writeFile(dir, "crlf.txt", joined(rows100, "\r\n"))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("maxPartitionBytes" -> "13"))(file.toString)
      assert(df.rdd.getNumPartitions >= 100,
        s"Expected ~108 one-split partitions, got ${df.rdd.getNumPartitions}")
      assertExactly100(df)
    }
  }

  // d
  test("LF control file split by maxPartitionBytes=500 yields exactly 100 unique rows") {
    withTempDir { dir =>
      val file = writeFile(dir, "lf.txt", joined(rows100, "\n"))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("maxPartitionBytes" -> "500"))(file.toString)
      assert(df.rdd.getNumPartitions >= 2)
      assertExactly100(df)
    }
  }

  // e
  test("CRLF file without trailing CRLF on the last line, split, yields 100 unique rows") {
    withTempDir { dir =>
      val file = writeFile(dir, "crlf_notrail.txt", joined(rows100, "\r\n", trailing = false))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("maxPartitionBytes" -> "500"))(file.toString)
      assert(df.rdd.getNumPartitions >= 2)
      assertExactly100(df)
    }
  }

  // f
  test("CRLF file with header, header=true, split: 100 rows, header never leaks") {
    withTempDir { dir =>
      val header = "NAME   ID   " // 12 chars like a data line; "ID" would fail the Int cast
      val file = writeFile(dir, "crlf_header.txt", header + "\r\n" + joined(rows100, "\r\n"))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("maxPartitionBytes" -> "500", "header" -> "true"))(file.toString)
      assert(df.rdd.getNumPartitions >= 2)
      val names = df.collect().map(_.getAs[String]("name")).toSet
      assert(!names.contains("NAME"), "Header line must never be emitted as data")
      assertExactly100(df)
    }
  }

  // g
  test("CRLF vs LF parity: identical content incl. 2 empty lines reads identically") {
    withTempDir { dir =>
      val lines = rows100.take(10) ++ Seq("") ++ rows100.slice(10, 50) ++ Seq("") ++ rows100.drop(50)
      val lf = writeFile(dir, "lf.txt", joined(lines, "\n"))
      val crlf = writeFile(dir, "crlf.txt", joined(lines, "\r\n"))

      def readSorted(p: JPath): Seq[Seq[String]] =
        readFixedWidth(FieldLengths, withCorruptSchema)(p.toString) // default planning => 1 partition
          .collect().map(_.toSeq.map(String.valueOf)).toSeq.sortBy(_.mkString("|"))

      val lfRows = readSorted(lf)
      val crlfRows = readSorted(crlf)
      assert(lfRows.length == 102, s"Expected 100 data + 2 empty-line rows, got ${lfRows.length}")
      assert(crlfRows == lfRows, s"CRLF and LF reads must be identical.\nlf:   $lfRows\ncrlf: $crlfRows")
    }
  }

  // h
  test("lineSep='|' uses an explicit custom record delimiter") {
    withTempDir { dir =>
      val file = writeFile(dir, "pipe.txt", "AAAAAAA00001|BBBBBBB00002|")
      val rows = readFixedWidth(FieldLengths, nameIdSchema,
        Map("lineSep" -> "|"))(file.toString).collect().sortBy(_.getAs[Int]("id"))
      assert(rows.length == 2, s"Expected 2 records delimited by '|', got ${rows.length}")
      assert(rows(0).getAs[String]("name") == "AAAAAAA" && rows(0).getAs[Int]("id") == 1)
      assert(rows(1).getAs[String]("name") == "BBBBBBB" && rows(1).getAs[Int]("id") == 2)
    }
  }

  // i
  test("lineSep='' is rejected with IllegalArgumentException") {
    withTempDir { dir =>
      val file = writeFile(dir, "crlf.txt", joined(rows100, "\r\n"))
      val ex = intercept[Exception] {
        readFixedWidth(FieldLengths, nameIdSchema, Map("lineSep" -> ""))(file.toString).collect()
      }
      assert(causeChain(ex).exists(t =>
        t.isInstanceOf[IllegalArgumentException] && t.getMessage.contains("lineSep")),
        s"Expected IllegalArgumentException mentioning lineSep, got: $ex")
    }
  }

  // j
  test("writer-only lineEnding option passed on read is ignored without error") {
    withTempDir { dir =>
      val file = writeFile(dir, "crlf.txt", joined(rows100, "\r\n"))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("lineEnding" -> "\n"))(file.toString)
      assertExactly100(df)
    }
  }

  // k
  test("splittable bz2 file read across several splits yields exactly 100 unique rows") {
    withTempDir { dir =>
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val codec = new org.apache.hadoop.io.compress.BZip2Codec()
      codec.setConf(hadoopConf) // compressor selection consults the conf
      val bz2 = dir.resolve("crlf.txt.bz2")
      val out = codec.createOutputStream(Files.newOutputStream(bz2))
      try out.write(joined(rows100, "\r\n").getBytes(StandardCharsets.UTF_8)) finally out.close()

      // Split by compressed size so the test always produces >= 3 PartitionedFile splits
      // regardless of how small bzip2 makes 1400 bytes of regular text.
      val compressedSize = Files.size(bz2)
      val splitBytes = math.max(1L, compressedSize / 3)
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("maxPartitionBytes" -> splitBytes.toString))(bz2.toString)
      assert(df.rdd.getNumPartitions >= 2,
        s"bz2 is a SplittableCompressionCodec: $compressedSize-byte file at $splitBytes-byte splits " +
          s"must yield >= 2 partitions, got ${df.rdd.getNumPartitions}")
      assertExactly100(df)
    }
  }

  // l
  test("Cp1047 (EBCDIC) LF-terminated file, split, yields 100 unique rows") {
    // EBCDIC LF is 0x25, not 0x0A. The reader must derive the delimiter from the charset.
    withTempDir { dir =>
      val file = writeFile(dir, "ebcdic.dat", joined(rows100, "\n"), Charset.forName("Cp1047"))
      val df = readFixedWidth(FieldLengths, withCorruptSchema,
        Map("maxPartitionBytes" -> "500", "encoding" -> "Cp1047"))(file.toString)
      assert(df.rdd.getNumPartitions >= 2)
      assertExactly100(df)
    }
  }
}
