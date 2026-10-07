// scripts/bench/read_bench.scala
// Run:  spark-shell --driver-memory 4g --jars target/scala-2.13/spark-fixedwidth-datasource_2.13-<version>.jar \
//         < scripts/bench/read_bench.scala
// (Pipe via stdin: spark-shell's -i/-I preload does not execute in non-interactive shells.)
// Generates (once) two 36-byte-per-line CRLF files with `bench.rows` rows:
//   clean.txt      every row valid
//   mostly_bad.txt 9 of 10 rows have a non-numeric id (exercises the rescued-data path)
// Times each query `bench.runs` times, prints the median wall time and the JVM GC time delta.
import java.io.{BufferedOutputStream, FileOutputStream}
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types._

val dir  = sys.props.getOrElse("bench.dir", "/tmp/fwbench")
val rows = sys.props.get("bench.rows").map(_.toInt).getOrElse(4279452)
val runs = sys.props.get("bench.runs").map(_.toInt).getOrElse(3)
Files.createDirectories(Paths.get(dir))

def gen(name: String, isBad: Int => Boolean): String = {
  val p = s"$dir/$name"
  if (!Files.exists(Paths.get(p))) {
    val out = new BufferedOutputStream(new FileOutputStream(p), 1 << 20)
    var i = 1
    while (i <= rows) {
      val id = if (isBad(i)) "BAD%07d".format(i % 10000000) else f"$i%010d"
      out.write((id + ("X" * 14) + f"${i % 100000}%010d" + "\r\n").getBytes("UTF-8")) // 34 + CRLF = 36 bytes
      i += 1
    }
    out.close()
  }
  p
}
val clean     = gen("clean.txt", _ => false)
val mostlyBad = gen("mostly_bad.txt", i => i % 10 != 0)

// NB: trailing-dot continuation is required — this script is piped into spark-shell's
// stdin (line-by-line interpretation), where leading-dot continuation lines detach.
val schema = StructType(Seq(
  StructField("id", LongType), StructField("pad", StringType), StructField("amount", LongType)))

def read(p: String) = spark.read.format("fixedwidth-custom-scala").
  option("field_lengths", "0:10,10:24,24:34").
  option("rescuedDataColumn", "_rescued_data").
  option("maxPartitionBytes", "134217728").
  schema(schema).load(p)

def gcMs: Long = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum

def timeIt(label: String)(f: => Long): Unit = {
  val samples = (1 to runs).map { _ =>
    val gc0 = gcMs; val t0 = System.nanoTime()
    val n = f
    ((System.nanoTime() - t0) / 1e6, gcMs - gc0, n)
  }
  val medianMs = samples.map(_._1).sorted.apply(runs / 2)
  println(f"$label%-26s rows=${samples.head._3}%-9d median=${medianMs}%7.0f ms  gc(ms)=${samples.map(_._2).mkString(",")}  all(ms)=${samples.map(_._1.toLong).mkString(",")}")
}

println(s"jar=${sys.props.getOrElse("spark.jars", "")} rows=$rows runs=$runs cores=${Runtime.getRuntime.availableProcessors}")
timeIt("clean count")(read(clean).count())
timeIt("clean sum(id)")(read(clean).agg(sum("id")).first().getLong(0))
timeIt("mostly_bad count")(read(mostlyBad).count())
timeIt("mostly_bad rescued rows")(read(mostlyBad).filter("_rescued_data IS NOT NULL").count())
System.exit(0)
