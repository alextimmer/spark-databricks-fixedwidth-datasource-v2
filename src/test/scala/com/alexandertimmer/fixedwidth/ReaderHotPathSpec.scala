// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._

import scala.util.Try

/**
 * Pins the exact semantics of the hot-path primitives introduced in 0.2.2.
 * Every helper here replaces an allocation- or exception-heavy construct and
 * must be indistinguishable from the original for all inputs.
 */
class ReaderHotPathSpec extends AnyFunSuite {

  private val fields = Array(
    StructField("s", StringType), StructField("i", IntegerType), StructField("l", LongType),
    StructField("f", FloatType), StructField("d", DoubleType), StructField("b", BooleanType),
    StructField("dt", DateType), StructField("ts", TimestampType), StructField("dec", DecimalType(10, 2)))

  private val rowsMatrix: Seq[Array[String]] = Seq(
    Array("abc", "42", "9000000000", "1.5", "2.5", "true", "2024-01-31", "2024-01-31 12:34:56", "12.34"),
    Array("", "", "", "", "", "", "", "", ""),
    Array(null, null, null, null, null, null, null, null, null),
    Array("x", "4.2", "12a", "NaN", "Inf", "yes", "31.01.2024", "garbage", "1,5"),
    Array("y", "2147483648", "-9223372036854775809", "-Inf", "nan", "FALSE", "2024-13-01", "2024-01-31T12:34:56", "99999999999.99"),
    Array("z", "+7", "-0", "abc", "-Inf", "True", "2024-02-29", "2024-02-29 00:00:00", "-0.01"))

  test("FieldCaster.cast equals FWUtils.cast for the whole matrix (default and custom formats)") {
    val configs = Seq(
      (None, None, None),
      (Some("dd.MM.yyyy"), Some("yyyy-MM-dd'T'HH:mm:ss"), Some("Europe/Berlin")))
    configs.foreach { case (df, tf, tz) =>
      val caster = new FWUtils.FieldCaster(fields, df, tf, tz, "NaN", "Inf", "-Inf")
      rowsMatrix.foreach { row =>
        val (expOut, expBad) = FWUtils.cast(row, fields, df, tf, tz, "NaN", "Inf", "-Inf")
        val (gotOut, gotBad) = caster.cast(row)
        assert(gotOut.toSeq.map(String.valueOf) == expOut.toSeq.map(String.valueOf), s"values differ for ${row.toSeq} / $df $tf $tz")
        assert(gotBad.toSeq == expBad.toSeq, s"badIndices differ for ${row.toSeq}")
      }
    }
  }

  test("stripLeadingSpace/stripTrailingSpace equal the former ^\\s+ / \\s+$ regexes") {
    val samples = Seq("", " ", "  a  ", "\t\n\u000B\f\r x \r\f\u000B\n\t", " nbsp ", "abc", " a b ",
      "\u001Cfs", "x\u0085nel", " em", "   ", "a\t")
    samples.foreach { s =>
      assert(FWUtils.stripLeadingSpace(s) == s.replaceAll("^\\s+", ""), s"leading differs for ${s.map(_.toInt)}")
      assert(FWUtils.stripTrailingSpace(s) == s.replaceAll("\\s+$", ""), s"trailing differs for ${s.map(_.toInt)}")
    }
  }

  test("parseLongOrNull/parseIntOrNull equal Scala toLong/toInt (null instead of exception)") {
    val samples = Seq("0", "7", "+7", "-7", "-0", "007", "", "-", "+", "+-1", " 5", "5 ", "1_0", "0x10", "1.0", "1e3",
      "12a", "a12", "2147483647", "2147483648", "-2147483648", "-2147483649",
      "9223372036854775807", "9223372036854775808", "-9223372036854775808", "-9223372036854775809",
      "99999999999999999999999", "٣٤" /* Arabic-Indic 34: Character.digit accepts it, so does parseInt */,
      "１２" /* fullwidth 12 */, "١٢٣")
    samples.foreach { s =>
      val expLong = Try(s.toLong).toOption.map(Long.box).orNull
      val expInt = Try(s.toInt).toOption.map(Int.box).orNull
      assert(FWUtils.parseLongOrNull(s) == expLong, s"long differs for '$s'")
      assert(FWUtils.parseIntOrNull(s) == expInt, s"int differs for '$s'")
    }
  }

  test("parseBooleanOrNull equals Scala toBoolean (null instead of exception)") {
    val samples = Seq("true", "TRUE", "True", "tRuE", "false", "FALSE", "False", "yes", "no", "1", "0", "", " true", "true ", "t", "f")
    samples.foreach { s =>
      val exp = Try(s.toBoolean).toOption.map(Boolean.box).orNull
      assert(FWUtils.parseBooleanOrNull(s) == exp, s"boolean differs for '$s'")
    }
  }
}
