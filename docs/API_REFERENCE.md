# API Reference

Complete API reference for the Spark Fixed-Width Data Source library.

---

## DataSource Registration

### Format Name

```python
spark.read.format("fixedwidth-custom-scala")
```

The data source is registered under the short name `fixedwidth-custom-scala` via Java ServiceLoader.

---

## Read API

### Basic Usage

```python
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .schema(schema) \
    .load("/path/to/file.txt")
```

### Methods

#### `.format(name: str)`

Specifies the data source format.

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Must be `"fixedwidth-custom-scala"` |

#### `.option(key: str, value: str)`

Sets a single option.

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | `str` | Option name (case-insensitive) |
| `value` | `str` | Option value |

#### `.options(**kwargs)`

Sets multiple options.

```python
df = spark.read.format("fixedwidth-custom-scala") \
    .options(
        field_lengths="0:10,10:20",
        mode="PERMISSIVE",
        encoding="UTF-8"
    ) \
    .load("/path/to/file.txt")
```

#### `.schema(schema: StructType)`

Specifies the schema for the data.

| Parameter | Type | Description |
|-----------|------|-------------|
| `schema` | `StructType` | Spark SQL schema definition |

If not provided, schema is inferred from `field_lengths` count with all `StringType` columns.

#### `.load(path: str)`

Loads data from the specified path. Supports glob patterns.

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | File path, directory, or glob pattern |

**Path Examples:**
```python
# Single file
.load("/data/file.txt")

# Directory (all files)
.load("/data/fixed_width/")

# Glob pattern
.load("/data/files_*.txt")

# Multiple patterns
.load("/data/2024-*/report.txt")

# Compressed files
.load("/data/file.txt.gz")
```

---

## Write API

### Basic Usage

```python
df.write.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .mode("overwrite") \
    .save("/path/to/output/")
```

### Methods

#### `.mode(saveMode: str)`

Specifies the behavior when output path exists.

| Mode | Behavior |
|------|----------|
| `"append"` | Append to existing files |
| `"overwrite"` | Overwrite existing files |
| `"error"` / `"errorifexists"` | Error if path exists (default) |
| `"ignore"` | Ignore if path exists |

#### `.save(path: str)`

Saves the DataFrame to the specified path.

### Writer Options

#### `paddingChar`

Character used to pad field values to their required width.

| Property | Value |
|----------|-------|
| **Default** | `" "` (space) |
| **Type** | Single character |

```python
.option("paddingChar", "0")   # Zero-pad fields
.option("paddingChar", " ")   # Space-pad (default)
.option("paddingChar", "_")   # Underscore-pad
```

#### `alignment`

Alignment of field values within their fixed-width columns.

| Property | Value |
|----------|-------|
| **Default** | `"left"` |
| **Values** | `"left"`, `"right"` |

```python
.option("alignment", "right")  # Right-align (pad on left)
.option("alignment", "left")   # Left-align (pad on right, default)
```

**Combine with `paddingChar` for common patterns:**
```python
# Right-aligned zero-padded numbers: "000042"
.option("alignment", "right").option("paddingChar", "0")
```

#### `lineEnding`

Line ending sequence for output files.

| Property | Value |
|----------|-------|
| **Default** | System line separator (`System.lineSeparator()`) |
| **Type** | String |

```python
.option("lineEnding", "\n")     # Unix (LF)
.option("lineEnding", "\r\n")   # Windows (CRLF)
```

#### Shared Options

The following options are shared between read and write paths:

| Option | Read Behavior | Write Behavior |
|--------|---------------|----------------|
| `dateFormat` | Parse dates from text | Format dates to text |
| `timestampFormat` | Parse timestamps from text | Format timestamps to text (default: `yyyy-MM-dd HH:mm:ss`) |
| `timeZone` | Timezone for parsing | Timezone for formatting |

> **Note:** The writer uses `yyyy-MM-dd HH:mm:ss` as its default timestamp format (without `T` separator), which differs from the reader default of `yyyy-MM-dd'T'HH:mm:ss`.

---

## Configuration Options

### Required Options

#### `field_lengths`

Defines field positions using `start:end` pairs (0-indexed, end exclusive).

| Property | Value |
|----------|-------|
| **Required** | Yes (unless `field_simple` or schema metadata provided) |
| **Format** | `"start:end,start:end,..."` |
| **Example** | `"0:5,5:15,15:20"` |

```python
.option("field_lengths", "0:7,7:17,17:20,20:23,23:33")
```

**Semantics:**
- Uses Java `substring(start, end)` semantics
- Start is inclusive, end is exclusive
- Positions are 0-indexed

#### `field_simple`

Alternative field specification using widths instead of positions.

| Property | Value |
|----------|-------|
| **Required** | Alternative to `field_lengths` |
| **Format** | `"width1,width2,width3,..."` |
| **Example** | `"7,10,3,3,10"` |

```python
.option("field_simple", "7,10,3,3,10")
# Equivalent to: "0:7,7:17,17:20,20:23,23:33"
```

### Parse Options

#### `mode`

Parse mode for handling malformed rows.

| Property | Value |
|----------|-------|
| **Default** | `"PERMISSIVE"` |
| **Values** | `"PERMISSIVE"`, `"DROPMALFORMED"`, `"FAILFAST"` |

| Mode | Behavior |
|------|----------|
| `PERMISSIVE` | NULL for unparseable fields; populate rescue columns |
| `DROPMALFORMED` | Skip rows with parse errors |
| `FAILFAST` | Throw exception on first parse error |

#### `columnNameOfCorruptRecord`

Column name to store raw line when parsing fails.

| Property | Value |
|----------|-------|
| **Default** | None (column ignored unless in schema) |
| **Type** | `StringType` |

```python
.option("columnNameOfCorruptRecord", "_corrupt_record")
```

**Behavior:**
- If column is in schema: populated with raw line on failure
- If column NOT in schema: silently dropped (not auto-added)

#### `rescuedDataColumn`

Column name for JSON payload with rescued data.

| Property | Value |
|----------|-------|
| **Default** | None (auto-uses `_rescued_data` if in schema) |
| **Type** | `StringType` (JSON format) |

```python
.option("rescuedDataColumn", "_rescued_data")
```

**JSON Format:**
```json
{
  "field_name": "unparseable_value",
  "_file_path": "/path/to/file.txt"
}
```

**Behavior:**
- If column NOT in schema: auto-appended to schema
- If column in schema: populated with JSON on parse failure

### Encoding Options

#### `encoding`

Character encoding for reading files.

| Property | Value |
|----------|-------|
| **Default** | `"UTF-8"` |
| **Values** | Any Java charset name |

```python
.option("encoding", "ISO-8859-1")
.option("encoding", "UTF-16")
.option("encoding", "windows-1252")
```

### Header Options

#### `skip_lines`

Number of header lines to skip.

| Property | Value |
|----------|-------|
| **Default** | `0` |
| **Type** | Integer |

```python
.option("skip_lines", "1")  # Skip header row
.option("skip_lines", "3")  # Skip 3 header lines
```

#### `header`

Alias for `skip_lines=1`.

| Property | Value |
|----------|-------|
| **Default** | `"false"` |
| **Values** | `"true"`, `"false"` |

```python
.option("header", "true")  # Equivalent to skip_lines=1
```

### Whitespace Options

#### `trimValues`

Trim whitespace from all field values.

| Property | Value |
|----------|-------|
| **Default** | `"true"` |
| **Values** | `"true"`, `"false"` |

```python
.option("trimValues", "false")  # Preserve whitespace
```

#### `ignoreLeadingWhiteSpace`

Trim leading whitespace from field values.

| Property | Value |
|----------|-------|
| **Default** | `"true"` |
| **Values** | `"true"`, `"false"` |

#### `ignoreTrailingWhiteSpace`

Trim trailing whitespace from field values.

| Property | Value |
|----------|-------|
| **Default** | `"true"` |
| **Values** | `"true"`, `"false"` |

### Null Value Options

#### `nullValue`

String to interpret as SQL NULL.

| Property | Value |
|----------|-------|
| **Default** | None (no null substitution) |
| **Type** | String |

```python
.option("nullValue", "")        # Empty string → NULL
.option("nullValue", "NULL")    # "NULL" string → NULL
.option("nullValue", "N/A")     # "N/A" → NULL
```

### Date/Timestamp Options

#### `dateFormat`

Format pattern for parsing `DateType` columns.

| Property | Value |
|----------|-------|
| **Default** | `"yyyy-MM-dd"` |
| **Format** | Java `DateTimeFormatter` pattern |

```python
.option("dateFormat", "dd/MM/yyyy")
.option("dateFormat", "MM-dd-yyyy")
.option("dateFormat", "yyyyMMdd")
```

#### `timestampFormat`

Format pattern for parsing `TimestampType` columns.

| Property | Value |
|----------|-------|
| **Default** | `"yyyy-MM-dd'T'HH:mm:ss"` |
| **Format** | Java `DateTimeFormatter` pattern |

```python
.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
.option("timestampFormat", "dd/MM/yyyy HH:mm:ss.SSS")
```

#### `timeZone`

Timezone for date/timestamp parsing.

| Property | Value |
|----------|-------|
| **Default** | `"UTC"` |
| **Format** | Java timezone ID |

```python
.option("timeZone", "America/New_York")
.option("timeZone", "Europe/London")
.option("timeZone", "+05:30")
```

### Comment Options

#### `comment`

Character indicating comment lines to skip.

| Property | Value |
|----------|-------|
| **Default** | None (no comment handling) |
| **Type** | Single character |

```python
.option("comment", "#")
.option("comment", ";")
```

Lines starting with the comment character are skipped entirely.

### Partitioning Options

#### `maxPartitionBytes`

Maximum bytes per partition for large files.

| Property | Value |
|----------|-------|
| **Default** | `"134217728"` (128MB) |
| **Type** | Long (bytes) |

```python
.option("maxPartitionBytes", "67108864")  # 64MB partitions
.option("maxPartitionBytes", "268435456") # 256MB partitions
```

#### `numPartitions`

Override: exact number of partitions.

| Property | Value |
|----------|-------|
| **Default** | Auto-calculated |
| **Type** | Integer |

```python
.option("numPartitions", "8")  # Exactly 8 partitions
```

---

## Schema API

### Schema with Metadata

Field widths can be specified via schema metadata instead of options.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True, metadata={"width": 10}),
    StructField("id", IntegerType(), True, metadata={"width": 5}),
    StructField("code", StringType(), True, metadata={"width": 3})
])

# No field_lengths option needed
df = spark.read.format("fixedwidth-custom-scala") \
    .schema(schema) \
    .load("/path/to/file.txt")
```

**Metadata Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `width` | Long | Field width in characters |

### Special Columns

Include these columns in your schema for error handling:

| Column Name | Purpose | Auto-Added? |
|-------------|---------|-------------|
| `_corrupt_record` | Raw line on parse failure | No (must be in schema) |
| `_rescued_data` | JSON with rescued values | Yes (if option set) |

```python
schema = StructType([
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])
```

---

## Supported Data Types

### Type Conversion

| Spark Type | Read (Input Format) | Write (Output Format) |
|------------|--------------------|-----------------------|
| `StringType` | Any text | As-is |
| `IntegerType` | Integer (`"12345"`) | `toString` |
| `LongType` | Long (`"9876543210"`) | `toString` |
| `FloatType` | Float (`"123.45"`) | `toString` |
| `DoubleType` | Double (`"123.456789"`) | `toString` |
| `DecimalType(p,s)` | Decimal (`"12345.67"`) | `toPlainString` (no scientific notation) |
| `BooleanType` | `"true"` / `"false"` | `toString` |
| `DateType` | Configurable via `dateFormat` | Configurable via `dateFormat` |
| `TimestampType` | Configurable via `timestampFormat` | Configurable via `timestampFormat` |
| `BinaryType` | Raw bytes | `toString` |
| `ByteType` | Byte (`"127"`) | `toString` |
| `ShortType` | Short (`"32767"`) | `toString` |

### Type Coercion Rules

1. **Leading/trailing whitespace** is trimmed before conversion (default)
2. **Empty string** becomes NULL for numeric types
3. **Invalid format** becomes NULL in PERMISSIVE mode
4. **NULL input** remains NULL

---

## Error Handling

### Exception Types

| Exception | Cause | Mode |
|-----------|-------|------|
| `SparkException` | Parse error in FAILFAST mode | FAILFAST |
| `IllegalArgumentException` | Invalid options | All |
| `FileNotFoundException` | Path not found | All |

### Error Recovery

```python
try:
    df = spark.read.format("fixedwidth-custom-scala") \
        .option("field_lengths", "0:10,10:20") \
        .option("mode", "FAILFAST") \
        .load("/path/to/file.txt")
    df.collect()
except Exception as e:
    print(f"Parse error: {e}")
```

### Rescued Data JSON Schema

```json
{
  "type": "object",
  "properties": {
    "_file_path": {"type": "string"},
    "<field_name>": {"type": "string"}
  }
}
```

---

## Code Examples

### PySpark Examples

#### Basic Read

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True)
])

df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:15,15:18") \
    .schema(schema) \
    .load("/data/records.txt")

df.show()
```

#### With Error Handling

```python
schema = StructType([
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])

df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("rescuedDataColumn", "_rescued_data") \
    .schema(schema) \
    .load("/data/records.txt")

# Find malformed records
df.filter("_rescued_data IS NOT NULL").show(truncate=False)
```

#### Custom Date Parsing

```python
from pyspark.sql.types import DateType, TimestampType

schema = StructType([
    StructField("event_date", DateType(), True),
    StructField("event_time", TimestampType(), True)
])

df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:29") \
    .option("dateFormat", "dd/MM/yyyy") \
    .option("timestampFormat", "dd/MM/yyyy HH:mm:ss") \
    .option("timeZone", "Europe/London") \
    .schema(schema) \
    .load("/data/events.txt")
```

#### Glob Patterns

```python
# Read all .txt files in directory
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/data/monthly/*.txt")

# Read files matching pattern
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/data/report_2024-*.txt")
```

### Scala Examples

#### Basic Read

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("name", StringType, nullable = true),
  StructField("id", IntegerType, nullable = true),
  StructField("code", StringType, nullable = true)
))

val df = spark.read.format("fixedwidth-custom-scala")
  .option("field_lengths", "0:10,10:15,15:18")
  .schema(schema)
  .load("/data/records.txt")

df.show()
```

#### Schema Inference

```scala
// No schema provided - infers from field_lengths
val df = spark.read.format("fixedwidth-custom-scala")
  .option("field_lengths", "0:10,10:20,20:30")
  .load("/data/records.txt")

df.printSchema()
// root
//  |-- col1: string (nullable = true)
//  |-- col2: string (nullable = true)
//  |-- col3: string (nullable = true)
```

#### Write Support

```scala
val df = spark.createDataFrame(Seq(
  ("Alice", 100, "A"),
  ("Bob", 200, "B")
)).toDF("name", "amount", "code")

// Basic write
df.write.format("fixedwidth-custom-scala")
  .option("field_lengths", "0:10,10:20,20:25")
  .mode("overwrite")
  .save("/output/fixed_width/")

// Right-aligned with zero-padding
df.write.format("fixedwidth-custom-scala")
  .option("field_lengths", "0:10,10:20,20:25")
  .option("alignment", "right")
  .option("paddingChar", "0")
  .option("lineEnding", "\n")
  .mode("overwrite")
  .save("/output/zero_padded/")
```

#### Write with PySpark

```python
df.write.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:25") \
    .option("paddingChar", "0") \
    .option("alignment", "right") \
    .option("dateFormat", "yyyyMMdd") \
    .option("timestampFormat", "yyyyMMddHHmmss") \
    .mode("overwrite") \
    .save("/output/fixed_width/")
```

---

## See Also

- [Configuration Guide](CONFIGURATION.md)
- [Architecture](ARCHITECTURE.md)
- [Troubleshooting](TROUBLESHOOTING.md)
- [Feature Roadmap](FEATURE_ROADMAP.md)
