# Troubleshooting Guide

Common issues and solutions for the Spark Fixed-Width Data Source.

---

## Quick Diagnostics

```python
# Check JAR is loaded
spark.sparkContext.getConf().get("spark.jars")

# Verify schema resolution
df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .load("/path/to/file.txt")
df.printSchema()

# Check partition count
df.rdd.getNumPartitions()
```

---

## JAR & ServiceLoader Errors

### "Failed to find data source: fixedwidth-custom-scala"

**Cause:** JAR not on classpath or ServiceLoader registration missing.

```python
# PySpark shell
pyspark --jars /path/to/spark-fixedwidth-datasource_2.13-0.1.0-SNAPSHOT.jar

# spark-submit
spark-submit --jars /path/to/spark-fixedwidth-datasource.jar your_script.py

# Programmatically (must be set before SparkSession is created)
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /path/to/jar.jar pyspark-shell'
```

**Verify ServiceLoader registration in JAR:**
```bash
jar tf target/scala-2.13/*.jar | grep services
# Should show: META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
```

---

## Field Definition Errors

### "Either 'field_simple' or 'field_lengths' is required"

```python
# Use one of these:
.option("field_lengths", "0:10,10:20,20:30")  # start:end pairs
.option("field_simple", "10,10,10")            # widths only
```

### "Invalid segment: 0-10"

Use colons, not hyphens: `"0:10,10:20"` not `"0-10,10-20"`.

---

## Data Parsing Issues

### All numeric/date columns are NULL

Read as `StringType` first to inspect raw values:

```python
from pyspark.sql.types import StructType, StructField, StringType

string_schema = StructType([
    StructField("field1", StringType(), True),
    StructField("field2", StringType(), True),
])

df = spark.read.format("fixedwidth-custom-scala") \
    .option("field_lengths", "0:10,10:20") \
    .option("trimValues", "false") \
    .schema(string_schema) \
    .load("/path/to/file.txt")

df.show(truncate=False)  # Check raw values including whitespace
```

Common fixes:
- Enable trimming: `.option("trimValues", "true")`
- Set date format: `.option("dateFormat", "yyyyMMdd")`
- Set timestamp format: `.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")`

### Garbled / special characters

Check file encoding and set it explicitly:

```bash
file -bi /path/to/file.txt
# Output: text/plain; charset=iso-8859-1
```

```python
.option("encoding", "ISO-8859-1")  # or windows-1252, Cp1047, UTF-16, etc.
```

### `_corrupt_record` is always NULL

The column **must** be present in the schema (it is not auto-added):

```python
schema = StructType([
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True)  # REQUIRED in schema
])

.option("columnNameOfCorruptRecord", "_corrupt_record")
```

### Querying `_rescued_data` JSON

```python
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

df.withColumn("rescued", F.from_json("_rescued_data", MapType(StringType(), StringType()))) \
    .select(F.col("rescued")["_file_path"].alias("source_file")) \
    .show()
```

---

## Databricks

### ClassNotFoundException

```
Py4JJavaError: java.lang.ClassNotFoundException: com.alexandertimmer.fixedwidth.DefaultSource
```

1. Upload JAR to DBFS or workspace
2. Attach to cluster: **Cluster → Libraries → Install New → Upload JAR**
3. **Restart cluster**

### Unity Catalog Permissions

Use Volumes paths:

```python
.load("/Volumes/catalog/schema/volume/file.txt")
```

---

## Jupyter Notebooks

### Changes not reflected after `sbt package`

1. **Restart Jupyter kernel** (Kernel → Restart)
2. If still stale, restart the Jupyter server and re-run `./scripts/start_jupyter.sh`

### "Cannot modify configuration after SparkContext has started"

```python
spark.stop()

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars", "/path/to/jar.jar") \
    .getOrCreate()
```

---

## Diagnostic Helpers

```python
# Visualize field positions against a sample line
line = "Alice     0000000123USD"
positions = [(0,10), (10,20), (20,23)]
for i, (start, end) in enumerate(positions):
    print(f"Field {i+1}: '{line[start:end]}' (pos {start}:{end})")
```

---

## See Also

- [Configuration Guide](CONFIGURATION.md)
- [API Reference](API_REFERENCE.md)
- [Architecture](ARCHITECTURE.md)
