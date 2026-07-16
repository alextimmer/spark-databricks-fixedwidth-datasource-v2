# Spark Fixed-Width Data Source

> **Note:** The documentation in this repository was largely AI-generated under human guidance and may contain errors. The code itself was primarily written by the author but refactored using GPT 5.1.

A custom Apache Spark Data Source V2 for reading and writing fixed-width formatted text files, designed specifically for **Databricks / Apache Spark 4.0.x**.

**Why this exists:** Databricks and Spark have no built-in support for fixed-width (positional) file formats. This library fills that gap by providing a drop-in data source that follows the same interface and error-handling conventions as Spark's built-in CSV reader — so if you know how to use `spark.read.format("csv")`, you already know how to use this. It replicates exact CSV PERMISSIVE mode behavior, including `columnNameOfCorruptRecord` and `rescuedDataColumn`, making it easy to integrate into existing Databricks pipelines without learning a new error-handling model.

## Features

- **Spark 4.0 DataSource V2 API** — Modern, scalable implementation
- **Read & Write support** — Full roundtrip: read fixed-width files and write DataFrames back
- **Exact CSV PERMISSIVE mode behavior** — Drop-in replacement for CSV error handling patterns
- **Rescued data column support** — JSON payload with malformed field values and file path
- **Corrupt record handling** — Raw line capture for debugging
- **Comprehensive type support** — String, Int, Long, Float, Double, Decimal, Boolean, Date, Timestamp with graceful NULL fallback
- **CSV-compatible options** — `emptyValue`, `nanValue`, `positiveInf`, `negativeInf` matching Spark CSV behavior
- **Configurable formatting** — Custom date/timestamp formats, timezone, null values, trim/whitespace control, comment lines
- **Flexible field definition** — `field_lengths` (start:end), `field_simple` (widths), or schema metadata
- **Native FileScan machinery** — Built on the same Spark internals as the built-in CSV source (since 0.2.0)
- **Multi-file loads** — Explicit file lists (`.load(f1, f2, ...)`), glob patterns, directories, `pathGlobFilter`, `recursiveFileLookup`
- **Per-row file provenance** — `input_file_name()` and `_metadata.file_path` name the source file of every row
- **Parallel processing** — Native cross-file bin-packing (many small files → few partitions) plus `maxPartitionBytes` and `numPartitions` overrides
- **Partition-directory inference** — Hive-style `key=value/` directories become partition columns
- **Compression support** — Transparent gzip and bzip2 decompression
- **Writer options** — Configurable padding character, field alignment (left/right), line endings
- **Databricks compatible** — Tested against Spark 4.0.2 with Hadoop 3 bindings

## Quick Start

### PySpark Usage

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Define schema
schema = StructType([
    StructField("country", StringType(), True),
    StructField("partnumber", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("date", DateType(), True),
    StructField("_corrupt_record", StringType(), True),  # Optional: include for corrupt records
])

# Read fixed-width file
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:7,7:17,17:20,20:23,23:33") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("rescuedDataColumn", "_rescued_data") \
    .schema(schema) \
    .load("/path/to/fixed_width_file.txt")

df.show(truncate=False)
```

### Multi-File Loads (since 0.2.0)

Multi-file reads behave exactly like the CSV source — per-file semantics for
error handling, with full row-level provenance:

```python
from pyspark.sql.functions import input_file_name, col

# Explicit file list — same options/schema as a single-file read
df = spark.read.format("fixedwidth-custom-scala") \
    .options(**file_options).schema(read_schema) \
    .load([file1, file2, file3])

# Glob, directory, or filtered directory reads
df = spark.read.format("fixedwidth-custom-scala") \
    .options(**file_options).schema(read_schema) \
    .option("pathGlobFilter", "*.txt") \
    .option("recursiveFileLookup", "true") \
    .load("/path/to/deliveries/")

# Which file did each row come from?
df.select("*", input_file_name().alias("source_file"))
df.select("*", col("_metadata.file_path"))
```

Small files are bin-packed across partitions automatically (no more one task
per tiny file), and a misaligned row lands in `rescuedDataColumn` exactly as
it would in a single-file load of that file.

### Scala Usage

```scala
import org.apache.spark.sql.types._

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
  .load("data/test_data/sample.txt")
```

### Using `field_simple` (Widths Instead of Positions)

```python
# Equivalent to field_lengths="0:7,7:17,17:20,20:23,23:33"
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_simple", "7,10,3,3,10") \
    .schema(schema) \
    .load("/path/to/file.txt")
```

### Writing Fixed-Width Files

```python
# Write DataFrame as fixed-width format
df.write.format("fixedwidth-custom-scala") \
    .option("field_simple", "10,15,5") \
    .option("paddingChar", " ") \
    .option("alignment", "left") \
    .mode("overwrite") \
    .save("/path/to/output/")
```

```scala
// Scala write with right-alignment (numeric padding)
df.write.format("fixedwidth-custom-scala")
  .option("field_simple", "10,15,5")
  .option("paddingChar", "0")
  .option("alignment", "right")
  .mode("overwrite")
  .save("/path/to/output/")
```

## Installation

### Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd sparkfixedwidthdatasource-scala

# Build the JAR
sbt package

# JAR location
ls target/scala-2.13/spark-fixedwidth-datasource_2.13-0.2.0-SNAPSHOT.jar
```

### PySpark Local Installation

#### Option 1: PySpark Shell with JAR

```bash
# Start PySpark shell with the JAR
pyspark --jars target/scala-2.13/spark-fixedwidth-datasource_2.13-0.2.0-SNAPSHOT.jar

# Now you can use the data source
>>> df = spark.read.format("fixedwidth-custom-scala") \
...     .option("field_lengths", "0:10,10:20") \
...     .load("data/test_data/valid1.txt")
```

#### Option 2: spark-submit for Scripts

```bash
# Run a PySpark script with the JAR
spark-submit \
    --jars target/scala-2.13/spark-fixedwidth-datasource_2.13-0.2.0-SNAPSHOT.jar \
    your_script.py
```

#### Option 3: Configure SparkSession Programmatically

```python
from pyspark.sql import SparkSession

# Create Spark session with JAR
spark = SparkSession.builder \
    .appName("FixedWidthReader") \
    .config("spark.jars", "/path/to/spark-fixedwidth-datasource_2.13-0.2.0-SNAPSHOT.jar") \
    .getOrCreate()

# Use the data source
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/path/to/file.txt")
```

#### Option 4: Environment Variable (Jupyter Notebooks)

```bash
# Set before starting Jupyter or Python
export PYSPARK_SUBMIT_ARGS="--jars /path/to/spark-fixedwidth-datasource_2.13-0.2.0-SNAPSHOT.jar pyspark-shell"

# Then start Jupyter
jupyter lab
```

### Databricks Installation

See [Databricks Deployment](#databricks-deployment) section below for detailed instructions on:
- Uploading the JAR to DBFS or Workspace
- Attaching to a cluster
- Using in Databricks notebooks

### Verifying Installation

After loading the JAR, verify the data source is available:

```python
# This should not raise an error
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:5") \
    .load("/path/to/any/file.txt")

# Check schema
df.printSchema()
```

If you get `Failed to find data source: fixedwidth-custom-scala`, the JAR is not loaded correctly. See [Troubleshooting](#troubleshooting) for solutions.

## Configuration Options

### Read Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `field_lengths` | Yes* | — | Field positions as `"start:end,start:end,..."` (0-indexed, end exclusive) |
| `field_simple` | Yes* | — | Alternative: field widths as `"10,15,5"` (auto-calculates positions) |
| `mode` | No | `PERMISSIVE` | Parse mode: `PERMISSIVE`, `DROPMALFORMED`, or `FAILFAST` |
| `columnNameOfCorruptRecord` | No | — | Column name to store raw line on parse failure |
| `rescuedDataColumn` | No | — | Column name for JSON payload with malformed values |
| `skip_lines` | No | `0` | Number of header lines to skip |
| `header` | No | `false` | Alias for `skip_lines=1` |
| `encoding` | No | `UTF-8` | Character encoding (UTF-8, ISO-8859-1, Cp1047, etc.) |
| `trimValues` | No | `true` | Trim leading and trailing whitespace from all fields |
| `ignoreLeadingWhiteSpace` | No | `true` | Trim only leading whitespace |
| `ignoreTrailingWhiteSpace` | No | `true` | Trim only trailing whitespace |
| `nullValue` | No | `""` | String value to interpret as SQL NULL. Default empty string: empty fields become NULL (CSV parity) |
| `emptyValue` | No | — | Value to substitute for empty StringType fields (after trim). Only takes effect if `nullValue` is set to something other than `""` (nullValue has precedence) |
| `nanValue` | No | `NaN` | String parsed as Float/Double NaN |
| `positiveInf` | No | `Inf` | String parsed as positive infinity |
| `negativeInf` | No | `-Inf` | String parsed as negative infinity |
| `dateFormat` | No | `yyyy-MM-dd` | Java DateTimeFormatter pattern for `DateType` columns |
| `timestampFormat` | No | `yyyy-MM-dd'T'HH:mm:ss` | Java DateTimeFormatter pattern for `TimestampType` columns |
| `timeZone` | No | `UTC` | Timezone for date/timestamp parsing |
| `comment` | No | — | Character marking comment lines to skip (e.g., `"#"`) |
| `maxPartitionBytes` | No | `134217728` | Maximum bytes per partition (128 MB). When unset, native Spark planning applies (session confs + cross-file bin-packing) |
| `numPartitions` | No | Auto | Single file: exact partition count. Multiple files: global target (`ceil(totalBytes/n)` split size). Compressed files are never split |
| `pathGlobFilter` | No | — | Glob pattern to filter file names when reading a directory (e.g., `"*.txt"`) |
| `recursiveFileLookup` | No | `false` | Read files in nested subdirectories (disables partition-directory inference) |

\* One of `field_lengths`, `field_simple`, or schema metadata widths is required.

### Write Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `field_lengths` | Yes* | — | Field positions (used to determine widths) |
| `field_simple` | Yes* | — | Alternative: field widths |
| `paddingChar` | No | Space | Character used for padding fields to width |
| `alignment` | No | `left` | Field alignment: `left` (right-pad) or `right` (left-pad) |
| `lineEnding` | No | Platform | Line ending: `\n`, `\r\n`, or platform default |
| `ignoreLeadingWhiteSpace` | No | `true` | Trim leading whitespace before padding |
| `ignoreTrailingWhiteSpace` | No | `true` | Trim trailing whitespace before padding |
| `emptyValue` | No | — | If StringType value matches, write as empty (all padding) |
| `nanValue` | No | `NaN` | String written for Float/Double NaN |
| `positiveInf` | No | `Inf` | String written for positive infinity |
| `negativeInf` | No | `-Inf` | String written for negative infinity |
| `dateFormat` | No | `yyyy-MM-dd` | Format pattern for writing `DateType` values |
| `timestampFormat` | No | `yyyy-MM-dd HH:mm:ss` | Format pattern for writing `TimestampType` values |
| `timeZone` | No | `UTC` | Timezone for date/timestamp formatting |
| `encoding` | No | `UTF-8` | Character encoding for output files |

### Parse Modes

| Mode | Behavior |
|------|----------|
| `PERMISSIVE` (default) | NULLs inserted for fields that could not be parsed correctly |
| `DROPMALFORMED` | Drops lines containing fields that could not be parsed |
| `FAILFAST` | Aborts reading if any malformed data is found |

### Field Lengths Format

The `field_lengths` option uses Java `substring(start, end)` semantics:
- Start position is **inclusive** (0-indexed)
- End position is **exclusive**

```
Example: "0:5,5:10,10:15"

Row: "Alice00001Hello"
      ├───┤├────┤├────┤
      0-4  5-9   10-14

Extracts: ["Alice", "00001", "Hello"]
```

## CSV PERMISSIVE Mode Compatibility

This library replicates all 6 documented CSV PERMISSIVE mode behaviors:

| Scenario | Option(s) Set | Column in Schema? | Behavior |
|----------|---------------|-------------------|----------|
| 1 | `columnNameOfCorruptRecord` | ✅ Yes | Populated with raw line on failure; NULL otherwise |
| 2 | `columnNameOfCorruptRecord` | ❌ No | Column **silently dropped** (not auto-added) |
| 3 | `rescuedDataColumn` | ❌ No | Column **auto-appended** with JSON on failure; NULL otherwise |
| 4 | `rescuedDataColumn` | ✅ Yes | Retained and populated with JSON on failure |
| 5 | Both options | Neither | Only `_rescued_data` appended; `_corrupt_record` dropped |
| 6 | Both options | Both | `_corrupt_record`=NULL, `_rescued_data`=JSON (rescued takes precedence) |

> **Auto-detection**: If your schema contains a column named `_corrupt_record`, it is automatically used for error handling even without setting the `columnNameOfCorruptRecord` option — matching CSV's default behavior. **Note:** `_rescued_data` is NOT auto-detected; it requires an explicit `rescuedDataColumn` option to be set.

### Rescued Data JSON Format

When a row has type conversion failures, the rescued data column contains:

```json
{
  "amount": "k153",
  "date": "2023-04-01A",
  "_file_path": "dbfs:/path/to/file.txt"
}
```

## Supported Data Types

| Spark Type | Read Behavior | Write Behavior |
|------------|---------------|----------------|
| `StringType` | Direct assignment | Direct output |
| `IntegerType` | Parse as integer; NULL on failure | Integer string |
| `LongType` | Parse as long; NULL on failure | Long string |
| `FloatType` | Parse as float; NULL on failure | Float string |
| `DoubleType` | Parse as double; NULL on failure | Double string |
| `DecimalType(p,s)` | Parse as BigDecimal; NULL on failure | `toPlainString` (no scientific notation) |
| `BooleanType` | Parse as boolean; NULL on failure | `true`/`false` string |
| `DateType` | Parse using `dateFormat`; NULL on failure | Format using `dateFormat` |
| `TimestampType` | Parse using `timestampFormat`; NULL on failure | Format using `timestampFormat` + `timeZone` |

## Development Setup

### Prerequisites

- Docker Desktop (for DevContainer)
- VS Code with Dev Containers extension
- OR: JDK 17+, SBT 1.9+, Spark 4.0.2

### DevContainer Setup (Recommended)

This project includes a fully configured DevContainer for reproducible development:

1. **Open in VS Code**
   ```bash
   git clone <repository-url>
   cd sparkfixedwidthdatasource-scala
   code .
   ```

2. **Reopen in Container**
   - Press `Ctrl+Shift+P` → "Dev Containers: Reopen in Container"
   - Wait for container build (includes Spark 4.0.2, SBT, Python/Jupyter)

3. **Verify Setup**
   ```bash
   ./build_and_test.sh
   ```

### DevContainer Features

- **Base Image**: Ubuntu 24.04 with JDK 17
- **Spark**: 4.0.2 with Hadoop 3 bindings (`/opt/spark-4.0.2-bin-hadoop3`)
- **Scala**: 2.13 (Databricks/Spark 4.0 compatible)
- **Python**: 3.x with PySpark for notebook testing
- **Jupyter**: Pre-configured with JAR auto-loading

## Project Structure

```
sparkfixedwidthdatasource-scala/
├── src/
│   ├── main/
│   │   ├── scala/com/alexandertimmer/fixedwidth/
│   │   │   ├── DefaultSource.scala           # ServiceLoader entry point
│   │   │   ├── FixedWidthPartitionReader.scala  # Core reader logic (parsing, type casting)
│   │   │   ├── FixedWidthDataWriter.scala    # Core writer logic
│   │   │   ├── FixedWidthBatchWrite.scala    # Batch write coordinator
│   │   │   ├── FixedWidthWriteBuilder.scala  # Write builder
│   │   │   ├── FixedWidthConstants.scala     # Centralized constants and option keys
│   │   │   └── FWUtils.scala                 # Schema inference, type casting
│   │   ├── scala/org/apache/spark/sql/execution/datasources/v2/fixedwidth/
│   │   │   │                                 # internal Spark package (private[sql] FileScan access)
│   │   │   ├── FixedWidthDataSourceV2.scala  # TableProvider + DataSourceRegister, path/paths resolution
│   │   │   ├── FixedWidthFileTable.scala     # FileTable + SupportsMetadataColumns (_metadata)
│   │   │   ├── FixedWidthFileScanBuilder.scala  # FileScanBuilder (no data-column pruning)
│   │   │   ├── FixedWidthFileScan.scala      # TextBasedFileScan + partition-planning override
│   │   │   ├── FixedWidthFilePartitionReaderFactory.scala  # PartitionedFile → reader bridge
│   │   │   └── FixedWidthFileFormat.scala    # Throwing V1 fallback shim
│   │   └── resources/
│   │       └── META-INF/services/
│   │           └── org.apache.spark.sql.sources.DataSourceRegister
│   └── test/
│       └── scala/com/alexandertimmer/fixedwidth/
│           ├── FixedWidthRelationSpec.scala              # Core read/write tests (59 tests)
│           ├── FixedWidthDataSourceRegressionSpec.scala  # Regression tests (13 tests)
│           ├── MissingFunctionalitySpec.scala            # Writer + CSV options tests (38 tests)
│           └── FileScanMigrationSpec.scala               # FileScan migration + multi-file tests (26 tests)
├── data/test_data/                           # Sample fixed-width files
├── docs/                                     # Documentation
├── notebook/                                 # Jupyter notebooks for testing
├── scripts/                                  # Build and launch scripts
├── build.sbt                                 # SBT configuration
└── scratch/                                  # Reference implementations (not production)
```

### Source Code Overview

| File | Purpose |
|------|---------|
| `DefaultSource.scala` | ServiceLoader entry point, extends `FixedWidthDataSourceV2` |
| `FixedWidthDataSourceV2.scala` | DataSource V2 provider: `shortName()` registration, `path`/`paths` list resolution. Implements `TableProvider` directly (not `FileDataSourceV2`) so writes stay on the V2 path (SPARK-28396) |
| `FixedWidthFileTable.scala` | `FileTable`-based table: file discovery via `PartitioningAwareFileIndex`, `_metadata` column, write-safe schema resolution |
| `FixedWidthFileScanBuilder.scala` | Scan builder; keeps all data columns (positional parsing), honors partition-column pruning and `_metadata` |
| `FixedWidthFileScan.scala` | `TextBasedFileScan`: native bin-packing plus `numPartitions`/`maxPartitionBytes` overrides, driver-side option resolution |
| `FixedWidthFilePartitionReaderFactory.scala` | Bridges Spark's `PartitionedFile` splits to the reader; appends partition values and `_metadata` |
| `FixedWidthFileFormat.scala` | Minimal V1 fallback shim that throws (loud failure instead of silently wrong data) |
| `FixedWidthPartitionReader.scala` | Core reader logic: parses rows, handles type conversion, populates special columns |
| `FixedWidthDataWriter.scala` | Core writer logic: formats values, pads fields, handles alignment and line endings |
| `FixedWidthBatchWrite.scala` | Batch write coordinator |
| `FixedWidthWriteBuilder.scala` | Write builder for DataSource V2 |
| `FixedWidthConstants.scala` | Centralized constants: option keys, defaults, error messages |
| `FWUtils.scala` | Utility functions: schema inference, type casting, special column handling |

## Build & Test

### Shell Scripts

| Script | Purpose |
|--------|---------|
| `scripts/build_and_test.sh` | Compile, package JAR, and run ScalaTest suite |
| `scripts/start_jupyter.sh` | Launch Jupyter Lab with JAR auto-loaded via `PYSPARK_SUBMIT_ARGS` |

### SBT Commands

```bash
# Compile sources
sbt compile

# Build JAR (includes META-INF resources)
sbt package

# Clean build
sbt clean package

# Run unit tests
sbt test

# Full build + test
./build_and_test.sh
```

### JAR Location

After building, the JAR is located at:
```
target/scala-2.13/spark-fixedwidth-datasource_2.13-0.2.0-SNAPSHOT.jar
```

### Interactive Testing with Jupyter

```bash
# Start Jupyter (auto-loads JAR)
./start_jupyter.sh

# Open http://localhost:8888 in browser
# Navigate to notebook/test_spark_setup.ipynb
```

## Development Workflow

1. **Make code changes** in `src/main/scala/fixedwidth/`

2. **Rebuild JAR**
   ```bash
   sbt package
   ```

3. **Run unit tests**
   ```bash
   sbt test
   ```

4. **Test in Jupyter** (if running)
   - Restart kernel: Kernel → Restart
   - Re-run notebook cells

5. **Verify all tests pass**
   ```bash
   ./build_and_test.sh
   ```

## Test Coverage

The test suite validates all CSV PERMISSIVE mode behaviors, reader options, writer features, and roundtrip correctness:

```
Tests: succeeded 135, failed 0 (across 4 test classes; 1 test self-cancels
without a local bz2 fixture)

FixedWidthRelationSpec (59 tests):
- Core CSV PERMISSIVE mode scenarios (6 scenarios)
- Type conversion: String, Int, Long, Float, Double, Boolean
- Whitespace: trimValues, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace
- Null value handling, comment lines, date/timestamp/timezone parsing
- Schema inference, schema metadata widths, field_simple format
- Multi-file reading, glob patterns, compression (gzip, bzip2)
- Partitioning: numPartitions, maxPartitionBytes
- Write support, schema metadata propagation
- Header/skip_lines, encoding, short-line handling

FixedWidthDataSourceRegressionSpec (13 tests):
- Regression tests for core read/write roundtrips

MissingFunctionalitySpec (38 tests):
- Writer: paddingChar, alignment (left/right), lineEnding
- Writer: DecimalType formatting, DateType/TimestampType roundtrip
- Write-side trim: ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace
- emptyValue: read substitution, write handling, roundtrip, nullValue precedence
- nanValue/positiveInf/negativeInf: read parsing, write formatting, roundtrip
- Rescued data: _file_path config (default, enabled, disabled, mid-session change)
- Constants consolidation validation

FileScanMigrationSpec (26 tests):
- Characterization safety net (pre-migration behavior pinned)
- Explicit multi-path loads (.load(f1, f2)), paths option
- input_file_name() and _metadata.file_path provenance
- Cross-file bin-packing, numPartitions global target on multi-file loads
- pathGlobFilter, recursiveFileLookup, Hive-style partition-dir inference
- Multi-file error semantics ≡ per-file single loads (incl. misaligned files)
- V1 fallback shim, missing-path error behavior
```

## Troubleshooting

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Failed to find data source: fixedwidth-custom-scala` | JAR not on classpath | Run `sbt package`, restart Jupyter kernel |
| `Provider fixedwidth.FixedWidthRelation not found` | ServiceLoader misconfigured | Verify `META-INF/services` file contains `com.alexandertimmer.fixedwidth.DefaultSource` |
| `UnsupportedOperationException: source does not support user-specified schema` | Schema handling bug | Ensure `getTable()` doesn't modify user schema |

### JAR Not Loading in Jupyter

1. Check JAR exists: `ls target/scala-2.13/*.jar`
2. Rebuild: `sbt clean package`
3. Restart Jupyter: `Ctrl+C` → `./start_jupyter.sh`
4. Restart kernel in notebook

## Databricks Deployment

To use this library on Databricks:

1. **Build the JAR locally**
   ```bash
   sbt clean package
   ```

2. **Upload to Databricks**
   - Workspace → Create → Library → Upload JAR
   - Or use DBFS: `dbfs:/FileStore/jars/spark-fixedwidth-datasource_2.13-0.2.0-SNAPSHOT.jar`

3. **Attach to Cluster**
   - Cluster → Libraries → Install New → Choose uploaded JAR

4. **Use in Notebook**
   ```python
   df = spark.read.format("fixedwidth-custom-scala") \
       .option("field_lengths", "0:7,7:17,17:20") \
       .schema(your_schema) \
       .load("dbfs:/path/to/file.txt")
   ```

## Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Scala | 2.13 | Language (Databricks/Spark 4.0 compatible) |
| Apache Spark | 4.0.2 | Runtime (Hadoop 3 bindings) |
| SBT | 1.9.x | Build tool |
| ScalaTest | 3.x | Unit testing |
| Jackson | 2.x | JSON serialization for rescued data |

## Architecture

Since 0.2.0 the read path is built on Spark's native FileScan machinery
(`FileTable` / `TextBasedFileScan` / `FilePartitionReaderFactory`) — the same
foundation as the built-in CSV source. See `docs/ARCHITECTURE.md` for details.

```
┌────────────────────────────────────────────────────────────────┐
│                       Spark Application                        │
├────────────────────────────────────────────────────────────────┤
│  spark.read.format("fixedwidth-custom-scala")                  │
│           │                                                    │
│           ▼                                                    │
│  ┌───────────────────────┐   ServiceLoader discovers           │
│  │ DefaultSource extends │◄── via META-INF/services            │
│  │ FixedWidthDataSourceV2│   resolves path + paths list        │
│  └──────────┬────────────┘                                     │
│             │ getTable()                                       │
│             ▼                                                  │
│  ┌────────────────────────┐  FileIndex: globs, dirs, filters,  │
│  │  FixedWidthFileTable   │  partition-dir inference;          │
│  │ (FileTable, _metadata) │  scan and write capabilities       │
│  └──────┬──────────┬──────┘                                    │
│         │          │                                           │
│    READ │          │ WRITE (unchanged path)                    │
│         ▼          ▼                                           │
│  ┌─────────────┐ ┌───────────────┐                             │
│  │FixedWidth-  │ │ WriteBuilder  │                             │
│  │FileScan     │ │ +BatchWrite   │                             │
│  │(bin-packing,│ └──────┬────────┘                             │
│  │ overrides)  │        │                                      │
│  └────┬────────┘        ▼                                      │
│       │          ┌───────────────┐                             │
│       ▼          │  DataWriter   │  Formats values, pads       │
│  ┌─────────────┐ │  (per task)   │  fields, writes lines       │
│  │ Partition-  │ └───────────────┘                             │
│  │ Reader (per │                                               │
│  │ file split) │                                               │
│  └────┬────────┘                                               │
│       │                                                        │
│       ▼                                                        │
│  ┌─────────────────────┐                                       │
│  │      FWUtils        │    Schema inference, type casting,    │
│  │  (Utility Object)   │    special column handling            │
│  └─────────────────────┘                                       │
│                                                                │
│  ┌─────────────────────┐                                       │
│  │ FixedWidthConstants │    Centralized option keys,           │
│  │  (Constants)        │    defaults, error messages           │
│  └─────────────────────┘                                       │
└────────────────────────────────────────────────────────────────┘
```

## Future Ideas

These are not bugs or missing functionality — potential enhancements if use cases arise:

| Feature | Notes |
|---------|-------|
| Fixed-line-length mode (`line_length`) | Seek-based reading for records without newlines |
| Per-field alignment | Alignment specified per-field via schema metadata |
| Write-path compression | Compress output files (gzip/bzip2) |
| JSON/YAML schema definition | External schema definition file |
| Column name inference from header | Parse first line as field names |
| Batch-level error aggregation | Aggregate parse error counts per partition |

## License

This project is licensed under the [Apache License 2.0](LICENSE).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Ensure all tests pass: `./scripts/build_and_test.sh`
4. Submit a pull request

## Acknowledgments

This implementation is designed to match the behavior documented in Apache Spark's CSV reader for seamless migration of fixed-width file processing to modern Spark DataSource V2 patterns.
