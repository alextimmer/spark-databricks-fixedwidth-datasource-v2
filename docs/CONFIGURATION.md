# Configuration Guide

Complete configuration reference for the Spark Fixed-Width Data Source.

---

## Quick Reference

### Minimum Configuration

```python
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .load("/path/to/file.txt")
```

### Recommended Production Configuration

```python
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .option("mode", "PERMISSIVE") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("encoding", "UTF-8") \
    .option("trimValues", "true") \
    .schema(your_schema) \
    .load("/path/to/file.txt")
```

---

## Configuration Options Reference

### Field Definition Options

#### `field_lengths` (Primary)

| Property | Value |
|----------|-------|
| **Required** | Yes (unless `field_simple` or metadata used) |
| **Default** | None |
| **Type** | String |
| **Format** | `"start:end,start:end,..."` |

Defines field positions using start and end byte offsets.

**Semantics:**
- **Start**: Inclusive, 0-indexed
- **End**: Exclusive (like Java `substring()`)

**Examples:**
```python
# Three fields: 0-9, 10-19, 20-29 (10 chars each)
.option("field_lengths", "0:10,10:20,20:30")

# Variable widths: 5, 15, 3 characters
.option("field_lengths", "0:5,5:20,20:23")

# Non-contiguous (skip characters 10-14)
.option("field_lengths", "0:10,15:25,25:35")
```

**Visual Example:**
```
Line:    "Alice     0000001234USD"
Position: 0----5----10---15---20--

field_lengths="0:10,10:20,20:23"
Field 1: "Alice     " (positions 0-9)
Field 2: "0000001234" (positions 10-19)
Field 3: "USD"        (positions 20-22)
```

---

#### `field_simple` (Alternative)

| Property | Value |
|----------|-------|
| **Required** | Alternative to `field_lengths` |
| **Default** | None |
| **Type** | String |
| **Format** | `"width1,width2,width3,..."` |

Defines fields by width, starting at position 0.

**Examples:**
```python
# Three fields: 10, 10, 3 characters wide
.option("field_simple", "10,10,3")
# Equivalent to: "0:10,10:20,20:23"

# Fields: 7, 10, 3, 3, 10 characters
.option("field_simple", "7,10,3,3,10")
# Equivalent to: "0:7,7:17,17:20,20:23,23:33"
```

---

#### Schema Metadata (Alternative)

Field widths can be embedded in the schema metadata.

```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("name", StringType(), True, metadata={"width": 10}),
    StructField("id", StringType(), True, metadata={"width": 10}),
    StructField("code", StringType(), True, metadata={"width": 3})
])

# No field_lengths needed
df = spark.read.format("fixedwidth-custom-scala") \
    .schema(schema) \
    .load("/path/to/file.txt")
```

**Precedence Order:**
1. `field_simple` (highest priority)
2. `field_lengths`
3. Schema metadata widths (lowest priority)

---

### Parse Mode Options

#### `mode`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"PERMISSIVE"` |
| **Type** | String |
| **Values** | `"PERMISSIVE"`, `"DROPMALFORMED"`, `"FAILFAST"` |

Controls behavior when encountering parse errors.

| Mode | Behavior | Use Case |
|------|----------|----------|
| `PERMISSIVE` | Insert NULL for unparseable fields; populate rescue columns | Data exploration, ETL with error tracking |
| `DROPMALFORMED` | Skip entire row on parse error | Data cleaning, strict pipelines |
| `FAILFAST` | Throw exception on first error | Validation, strict data contracts |

**Examples:**
```python
# Default: permissive with error tracking
.option("mode", "PERMISSIVE")
.option("rescuedDataColumn", "_rescued_data")

# Strict mode: fail immediately on error
.option("mode", "FAILFAST")

# Silent drop: remove malformed rows
.option("mode", "DROPMALFORMED")
```

---

### Error Handling Options

#### `columnNameOfCorruptRecord`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | None (uses `_corrupt_record` if in schema) |
| **Type** | String |

Column to store raw line content when parsing fails.

**Behavior Matrix:**

| Option Set | Column in Schema | Result |
|------------|------------------|--------|
| Yes | Yes | Populated with raw line on error |
| Yes | No | **Silently dropped** (not auto-added) |
| No | Yes (named `_corrupt_record`) | Auto-detected and used |
| No | No | Not used |

**Example:**
```python
schema = StructType([
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True)  # Must be in schema
])

df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load("/path/to/file.txt")
```

---

#### `rescuedDataColumn`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | None (uses `_rescued_data` if in schema) |
| **Type** | String |

Column to store JSON payload with rescued field values.

**Behavior Matrix:**

| Option Set | Column in Schema | Result |
|------------|------------------|--------|
| Yes | No | **Auto-appended** to schema |
| Yes | Yes | Populated with JSON on error |
| No | Yes (named `_rescued_data`) | **NOT auto-detected** (column stays NULL) |
| No | No | Not used |

**Critical Note:** Unlike `_corrupt_record`, the `_rescued_data` column is NOT auto-detected from the schema. You must explicitly set the `rescuedDataColumn` option to enable rescued data population.

**JSON Payload Format:**
```json
{
  "amount": "ABC123",
  "date": "invalid-date",
  "_file_path": "hdfs:///data/records.txt"
}
```

**Example:**
```python
# Column auto-appended if not in schema
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .option("rescuedDataColumn", "_rescued_data") \
    .load("/path/to/file.txt")

# Filter for rows with errors
df.filter("_rescued_data IS NOT NULL").show()
```

---

### Encoding Options

#### `encoding`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"UTF-8"` |
| **Type** | String |

Character encoding for reading files.

**Common Values:**

| Encoding | Description |
|----------|-------------|
| `UTF-8` | Unicode (default, recommended) |
| `UTF-16` | Unicode with BOM |
| `ISO-8859-1` | Latin-1 (Western European) |
| `windows-1252` | Windows Latin-1 |
| `US-ASCII` | 7-bit ASCII |
| `Cp1047` | EBCDIC (mainframe) |

**Examples:**
```python
# Latin-1 encoding
.option("encoding", "ISO-8859-1")

# Mainframe EBCDIC
.option("encoding", "Cp1047")

# Windows encoding
.option("encoding", "windows-1252")
```

---

### Header Options

#### `skip_lines`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `0` |
| **Type** | Integer |

Number of lines to skip at the beginning of each file.

```python
# Skip single header row
.option("skip_lines", "1")

# Skip multiple header lines
.option("skip_lines", "3")
```

**Note:** Only skipped on the first partition of each file.

---

#### `header`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"false"` |
| **Type** | Boolean String |

Convenience option equivalent to `skip_lines=1`.

```python
.option("header", "true")  # Same as skip_lines=1
```

---

### Whitespace Options

#### `trimValues`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"true"` |
| **Type** | Boolean String |

Trim leading and trailing whitespace from all field values.

```python
# Default: trim whitespace
.option("trimValues", "true")

# Preserve whitespace (for fixed-format data)
.option("trimValues", "false")
```

---

#### `ignoreLeadingWhiteSpace`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"true"` |
| **Type** | Boolean String |

Trim only leading whitespace from field values.

---

#### `ignoreTrailingWhiteSpace`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"true"` |
| **Type** | Boolean String |

Trim only trailing whitespace from field values.

**Precedence:**
- If `trimValues` is set, it overrides both `ignoreLeadingWhiteSpace` and `ignoreTrailingWhiteSpace`
- Individual options can be set when `trimValues` is not specified

---

### Null Value Options

#### `nullValue`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | None (no substitution) |
| **Type** | String |

String value to interpret as SQL NULL.

**Examples:**
```python
# Empty string becomes NULL
.option("nullValue", "")

# "NULL" string becomes NULL
.option("nullValue", "NULL")

# "N/A" becomes NULL
.option("nullValue", "N/A")

# Whitespace-only becomes NULL (after trimming)
.option("nullValue", "")
.option("trimValues", "true")
```

---

### Date/Timestamp Options

#### `dateFormat`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"yyyy-MM-dd"` |
| **Type** | String (Java DateTimeFormatter pattern) |

Format pattern for parsing `DateType` columns.

**Common Patterns:**

| Pattern | Example Input | Result |
|---------|---------------|--------|
| `yyyy-MM-dd` | `2025-01-15` | 2025-01-15 |
| `dd/MM/yyyy` | `15/01/2025` | 2025-01-15 |
| `MM-dd-yyyy` | `01-15-2025` | 2025-01-15 |
| `yyyyMMdd` | `20250115` | 2025-01-15 |
| `dd-MMM-yyyy` | `15-Jan-2025` | 2025-01-15 |

```python
.option("dateFormat", "dd/MM/yyyy")
```

---

#### `timestampFormat`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"yyyy-MM-dd'T'HH:mm:ss"` |
| **Type** | String (Java DateTimeFormatter pattern) |

Format pattern for parsing `TimestampType` columns.

**Common Patterns:**

| Pattern | Example Input |
|---------|---------------|
| `yyyy-MM-dd'T'HH:mm:ss` | `2025-01-15T10:30:00` |
| `yyyy-MM-dd HH:mm:ss` | `2025-01-15 10:30:00` |
| `dd/MM/yyyy HH:mm:ss` | `15/01/2025 10:30:00` |
| `yyyyMMddHHmmss` | `20250115103000` |
| `dd-MMM-yyyy HH:mm:ss.SSS` | `15-Jan-2025 10:30:00.123` |

```python
.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
```

---

#### `timeZone`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"UTC"` |
| **Type** | String (Java TimeZone ID) |

Timezone for interpreting date/timestamp values.

**Common Values:**

| Timezone ID | Description |
|-------------|-------------|
| `UTC` | Coordinated Universal Time |
| `America/New_York` | US Eastern |
| `America/Los_Angeles` | US Pacific |
| `Europe/London` | UK |
| `Europe/Berlin` | Central European |
| `Asia/Tokyo` | Japan |
| `+05:30` | UTC offset format |

```python
.option("timeZone", "America/New_York")
```

---

### Comment Options

#### `comment`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | None (no comment handling) |
| **Type** | Single character |

Character that marks comment lines to skip.

```python
# Skip lines starting with #
.option("comment", "#")

# Skip lines starting with ;
.option("comment", ";")

# Skip lines starting with *
.option("comment", "*")
```

**Behavior:** Lines where the first character (after any leading whitespace if trimming enabled) matches the comment character are entirely skipped.

---

### Partitioning Options

#### `maxPartitionBytes`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `134217728` (128 MB) |
| **Type** | Long (bytes) |

Maximum bytes per partition for splitting large files.

**Recommendations:**

| File Size | Recommended Setting |
|-----------|---------------------|
| < 128 MB | Default (single partition) |
| 128 MB - 1 GB | Default (auto-split) |
| > 1 GB | Consider increasing to 256 MB |
| > 10 GB | Tune based on cluster resources |

```python
# 64 MB partitions (more parallelism)
.option("maxPartitionBytes", "67108864")

# 256 MB partitions (less overhead)
.option("maxPartitionBytes", "268435456")
```

---

#### `numPartitions`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | Auto-calculated |
| **Type** | Integer |

Override: exact number of partitions for single-file reads.

```python
# Force exactly 8 partitions
.option("numPartitions", "8")

# Force exactly 16 partitions
.option("numPartitions", "16")
```

**Note:** For multi-file reads, each file gets one partition by default.

---

## Writer Options

Options specific to the write path (`df.write.format("fixedwidth-custom-scala")`).

### `paddingChar`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `" "` (space) |
| **Type** | Single character |

Character used to pad field values to their required width.

```python
.option("paddingChar", "0")   # Zero-pad: "42" → "000042" (right-aligned)
.option("paddingChar", " ")   # Space-pad: "42" → "42    " (left-aligned, default)
.option("paddingChar", "_")   # Underscore-pad
```

---

### `alignment`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | `"left"` |
| **Type** | String |
| **Values** | `"left"`, `"right"` |

Controls alignment of field values within their fixed-width columns.

| Alignment | Value `"42"` in 6-char field | Padding Position |
|-----------|-------------------------------|-----------------|
| `left` | `"42    "` | Right side |
| `right` | `"    42"` | Left side |

```python
# Right-aligned with zero-padding (common for numeric fields)
.option("alignment", "right")
.option("paddingChar", "0")
# Result: "42" → "000042"
```

---

### `lineEnding`

| Property | Value |
|----------|-------|
| **Required** | No |
| **Default** | System line separator (`System.lineSeparator()`) |
| **Type** | String |

Line ending sequence appended after each row.

```python
.option("lineEnding", "\n")     # Unix (LF)
.option("lineEnding", "\r\n")   # Windows (CRLF)
```

---

### Shared Read/Write Options

The following options apply to both read and write paths:

| Option | Read Default | Write Default | Description |
|--------|-------------|---------------|-------------|
| `field_lengths` | — | — | Field positions (required) |
| `dateFormat` | `yyyy-MM-dd` | `yyyy-MM-dd` | Date formatting pattern |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | `yyyy-MM-dd HH:mm:ss` | Timestamp formatting pattern |
| `timeZone` | `UTC` | `UTC` | Timezone for date/timestamp values |

> **Note:** The writer uses a different default for `timestampFormat` (space separator instead of `T`), which is more common for fixed-width output files.

---

## Configuration Scenarios

### Scenario 1: Basic Fixed-Width Read

```python
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .load("/data/records.txt")

df.show()
```

### Scenario 2: Production ETL with Error Tracking

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("order_amount", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("_rescued_data", StringType(), True)
])

df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .option("mode", "PERMISSIVE") \
    .option("rescuedDataColumn", "_rescued_data") \
    .option("dateFormat", "yyyyMMdd") \
    .option("nullValue", "") \
    .option("encoding", "UTF-8") \
    .schema(schema) \
    .load("/data/orders/*.txt")

# Separate good and bad records
good_records = df.filter("_rescued_data IS NULL")
bad_records = df.filter("_rescued_data IS NOT NULL")
```

### Scenario 3: Mainframe Data with EBCDIC

```python
schema = StructType([
    StructField("account_no", StringType(), True),
    StructField("balance", DecimalType(15, 2), True),
    StructField("last_update", DateType(), True)
])

df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:12,12:27,27:35") \
    .option("encoding", "Cp1047") \
    .option("dateFormat", "yyyyMMdd") \
    .option("nullValue", "        ") \
    .option("trimValues", "true") \
    .schema(schema) \
    .load("/data/mainframe/accounts.dat")
```

### Scenario 4: Large File with Parallel Processing

```python
# 10 GB file, split into 16 partitions
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .option("numPartitions", "16") \
    .load("/data/large_file.txt")

# Alternative: 64 MB auto-split
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .option("maxPartitionBytes", "67108864") \
    .load("/data/large_file.txt")
```

### Scenario 5: Multi-File Glob Pattern

```python
# All files in directory
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/data/daily_reports/")

# Files matching pattern
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/data/reports_2025-*.txt")

# Multiple directories
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/data/*/reports/*.txt")
```

### Scenario 6: Compressed Files

```python
# Gzip compressed
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/data/records.txt.gz")

# BZip2 compressed
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/data/records.txt.bz2")
```

**Note:** Compressed files cannot be split and are processed as single partitions.

### Scenario 7: Writing Fixed-Width Output

```python
# Basic write
df.write.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20,20:30") \
    .mode("overwrite") \
    .save("/output/fixed_width/")

# Right-aligned numeric output with zero-padding
df.write.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:20,20:35,35:43") \
    .option("alignment", "right") \
    .option("paddingChar", "0") \
    .option("dateFormat", "yyyyMMdd") \
    .option("lineEnding", "\n") \
    .mode("overwrite") \
    .save("/output/mainframe_format/")
```

---

## Best Practices

### Performance

1. **Provide explicit schema** - Avoids schema inference overhead
2. **Use appropriate partition size** - Balance parallelism vs. overhead
3. **Trim values** - Reduces memory for string processing
4. **Use PERMISSIVE mode with rescue columns** - Better than FAILFAST for large data

### Data Quality

1. **Always set `rescuedDataColumn`** - Track data quality issues
2. **Use appropriate `nullValue`** - Handle empty/placeholder values
3. **Validate date formats** - Test with sample data first
4. **Set encoding explicitly** - Don't rely on defaults for non-UTF-8 data

### Debugging

1. **Start with small sample** - Validate configuration on subset
2. **Use `df.printSchema()`** - Verify schema resolution
3. **Check `_rescued_data`** - Identify parsing issues
4. **Enable Spark logging** - For detailed diagnostics

---

## See Also

- [API Reference](API_REFERENCE.md)
- [Troubleshooting](TROUBLESHOOTING.md)
- [Architecture](ARCHITECTURE.md)
