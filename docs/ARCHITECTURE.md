# System Architecture

## Overview

The **Spark Fixed-Width Data Source** is a custom Apache Spark Data Source V2 implementation that enables reading fixed-width formatted text files with full support for Spark's PERMISSIVE mode error handling. This document provides a comprehensive technical overview of the system architecture, component interactions, data flow, and design decisions.

> **Purpose**: Enable enterprise-grade fixed-width file processing in Spark 4.0+ environments (including Databricks) with exact CSV PERMISSIVE mode behavior for error handling, rescued data columns, and corrupt record tracking.

---

## High-Level Architecture

```mermaid
flowchart TB
    subgraph SparkApp["Spark Application"]
        Client["spark.read.format('fixedwidth-custom-scala')"]
    end

    subgraph DSV2["DataSource V2 API Layer"]
        DS["DefaultSource<br/>(TableProvider)"]
        Table["FixedWidthTable<br/>(SupportsRead, SupportsWrite)"]
        Scan["FixedWidthScan<br/>(Scan + Batch)"]
    end

    subgraph Execution["Execution Layer"]
        Factory["FixedWidthPartitionReaderFactory"]
        Partition["FixedWidthPartition<br/>(InputPartition)"]
        Reader["FixedWidthPartitionReader<br/>(PartitionReader)"]
    end

    subgraph Utils["Utilities"]
        FWUtils["FWUtils<br/>(Schema, Casting, Parsing)"]
        Write["FixedWidthWriteBuilder<br/>(Optional Write Support)"]
    end

    subgraph Storage["Storage Layer"]
        FS["Hadoop FileSystem<br/>(Local, HDFS, S3, ADLS)"]
        Files["Fixed-Width Files<br/>(.txt, .dat, .gz, .bz2)"]
    end

    Client --> DS
    DS -->|"getTable()"| Table
    Table -->|"newScanBuilder()"| Scan
    Scan -->|"planInputPartitions()"| Partition
    Scan -->|"createReaderFactory()"| Factory
    Factory -->|"createReader()"| Reader
    Reader -->|"next(), get()"| FWUtils
    Reader --> FS
    FS --> Files
    Table -->|"newWriteBuilder()"| Write

    classDef api fill:#e1f5fe,stroke:#01579b
    classDef exec fill:#fff3e0,stroke:#e65100
    classDef util fill:#f3e5f5,stroke:#7b1fa2
    classDef storage fill:#e8f5e9,stroke:#2e7d32

    class DS,Table,Scan api
    class Factory,Partition,Reader exec
    class FWUtils,Write util
    class FS,Files storage
```

---

## Component Architecture

### 1. Entry Point Layer

#### DefaultSource.scala

| Property | Value |
|----------|-------|
| **Technology** | Scala 2.13, Spark DataSource V2 API |
| **Responsibilities** | ServiceLoader entry point, provider registration |
| **Key Dependencies** | `FixedWidthDataSource`, Java ServiceLoader |
| **Design Pattern** | Facade Pattern |

```scala
// Registered via META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
class DefaultSource extends TableProvider with DataSourceRegister {
  override def shortName(): String = "fixedwidth-custom-scala"
}
```

**ServiceLoader Discovery Flow:**
```
META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
    └── com.alexandertimmer.fixedwidth.DefaultSource
            └── shortName() = "fixedwidth-custom-scala"
```

---

#### FixedWidthDataSource.scala

| Property | Value |
|----------|-------|
| **Technology** | Spark DataSource V2 `TableProvider` API |
| **Responsibilities** | Schema inference, table creation, external metadata support |
| **Key Dependencies** | `FWUtils`, `FixedWidthTable` |
| **Design Pattern** | Factory Pattern |

**Key Methods:**

| Method | Purpose |
|--------|---------|
| `shortName()` | Returns `"fixedwidth-custom-scala"` for format registration |
| `supportsExternalMetadata()` | Returns `true` to accept user-provided schemas |
| `inferSchema(options)` | Infers base schema from `field_lengths` count |
| `getTable(schema, partitions, properties)` | Creates `FixedWidthTable` with resolved schema |

**Schema Resolution Strategy:**
```
User provides schema? ──┬── YES ──► Use user schema
                        │
                        └── NO ───► Call inferSchema()
                                         │
                                         ▼
                        appendSpecialColumns() applied to BOTH paths
                                         │
                                         ▼
                               Create FixedWidthTable(resolvedSchema)
```

---

### 2. Table Layer

#### FixedWidthTable.scala

| Property | Value |
|----------|-------|
| **Technology** | Spark `Table` with `SupportsRead` and `SupportsWrite` |
| **Responsibilities** | Define table capabilities, create scan/write builders |
| **Key Dependencies** | `FixedWidthScanBuilder`, `FixedWidthWriteBuilder` |
| **Design Pattern** | Builder Pattern |

**Capabilities:**
```scala
Set(
  TableCapability.BATCH_READ,      // Read in batches
  TableCapability.BATCH_WRITE,     // Write in batches
  TableCapability.ACCEPT_ANY_SCHEMA, // User schemas accepted
  TableCapability.TRUNCATE,        // Truncate on overwrite
  TableCapability.OVERWRITE_BY_FILTER,
  TableCapability.OVERWRITE_DYNAMIC
)
```

---

### 3. Scan Layer

#### FixedWidthScan.scala

| Property | Value |
|----------|-------|
| **Technology** | Spark `Scan` + `Batch` interfaces |
| **Responsibilities** | Partition planning, reader factory creation, glob expansion |
| **Key Dependencies** | `FixedWidthPartition`, `FixedWidthPartitionReaderFactory`, Hadoop FileSystem |
| **Design Pattern** | Strategy Pattern |

**Partition Planning Algorithm:**

```mermaid
flowchart TD
    A[Path Pattern] --> B{Expand Glob}
    B --> C[Get File Statuses]
    C --> D{For Each File}
    D --> E{Compressed?}
    E -->|Yes| F[Single Partition<br/>Entire File]
    E -->|No| G{Check Options}
    G --> H{numPartitions set?}
    H -->|Yes| I[Use Specified Count]
    H -->|No| J[Calculate from maxPartitionBytes]
    I --> K[Create N Partitions]
    J --> K
    K --> L[Byte-Range Partitions]
    F --> M[All Partitions]
    L --> M
```

**Key Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `maxPartitionBytes` | 128MB | Maximum bytes per partition |
| `numPartitions` | Auto | Override: exact partition count |

---

### 4. Partition Layer

#### FixedWidthPartition.scala

| Property | Value |
|----------|-------|
| **Technology** | Spark `InputPartition` |
| **Responsibilities** | Represent byte range in a file |
| **Key Dependencies** | None (data class) |
| **Design Pattern** | Value Object |

```scala
case class FixedWidthPartition(
  path: String,        // File path (HDFS, S3, local)
  start: Long,         // Byte offset start (inclusive)
  length: Long,        // Byte length of partition
  isFirstSplit: Boolean // True if this is first partition (no line skip)
) extends InputPartition
```

**Byte-Based Partitioning:**
```
File: [═══════════════════════════════════════════════════]
      │ Partition 1  │ Partition 2  │ Partition 3  │ P4  │
      └─ 0-128MB ────┴─ 128-256MB ──┴─ 256-384MB ──┴─ ... ┘

Partition N (non-first): Seeks to byte offset, skips partial line, reads to byte limit + complete line
```

---

### 5. Reader Layer

#### FixedWidthPartitionReader.scala

| Property | Value |
|----------|-------|
| **Technology** | Spark `PartitionReader[InternalRow]`, Hadoop FS API |
| **Responsibilities** | Line parsing, field extraction, type conversion, error handling |
| **Key Dependencies** | `FWUtils`, Hadoop `FSDataInputStream`, Jackson JSON |
| **Design Pattern** | Iterator Pattern |

**Key Features:**

| Feature | Implementation |
|---------|----------------|
| **Compression Support** | Auto-detect via `CompressionCodecFactory` (gzip, bzip2, etc.) |
| **Charset Support** | Configurable encoding (default: UTF-8) |
| **Line Trimming** | Configurable leading/trailing whitespace removal |
| **Null Handling** | Custom `nullValue` option for NULL representation |
| **Date/Timestamp** | Custom format patterns with timezone support |
| **Comment Lines** | Skip lines starting with comment character |
| **Error Modes** | PERMISSIVE, DROPMALFORMED, FAILFAST |

**Row Processing Flow:**

```mermaid
flowchart LR
    A[Read Line] --> B{Comment?}
    B -->|Yes| A
    B -->|No| C[Extract Fields<br/>by Position]
    C --> D[Apply Trim]
    D --> E{Check nullValue}
    E -->|Match| F[Set NULL]
    E -->|No Match| G[Cast to Type]
    G --> H{Success?}
    H -->|Yes| I[Add to Row]
    H -->|No| J[Handle by Mode]
    J -->|PERMISSIVE| K[NULL + Rescue]
    J -->|DROPMALFORMED| L[Skip Row]
    J -->|FAILFAST| M[Throw Exception]
    I --> N[InternalRow]
    K --> N
```

---

#### FixedWidthPartitionReaderFactory.scala

| Property | Value |
|----------|-------|
| **Technology** | Spark `PartitionReaderFactory` (Serializable) |
| **Responsibilities** | Create partition readers with configuration |
| **Key Dependencies** | `FixedWidthPartitionReader` |
| **Design Pattern** | Factory Pattern |

**Serializable Configuration:**
```scala
case class FixedWidthPartitionReaderFactory(
  schema: StructType,
  fieldLengths: String,
  mode: String,
  skipLines: Int,
  encoding: String,
  rescuedDataColumn: Option[String],
  columnNameOfCorruptRecord: Option[String],
  ignoreLeadingWhiteSpace: Boolean,
  ignoreTrailingWhiteSpace: Boolean,
  nullValue: Option[String],
  dateFormat: Option[String],
  timestampFormat: Option[String],
  timeZone: Option[String],
  comment: Option[Char]
) extends PartitionReaderFactory with Serializable
```

---

### 6. Utilities Layer

#### FWUtils.scala

| Property | Value |
|----------|-------|
| **Technology** | Pure Scala utility functions |
| **Responsibilities** | Schema inference, type casting, position parsing, special column handling |
| **Key Dependencies** | Spark SQL Types, Java DateTime API |
| **Design Pattern** | Utility Module (Singleton Object) |

**Key Functions:**

| Function | Purpose |
|----------|---------|
| `parsePositions(opts)` | Parse `field_lengths` or `field_simple` to position tuples |
| `parseFieldSimple(widths)` | Convert width list to cumulative positions |
| `inferBaseSchema(opts)` | Create schema from field count |
| `appendSpecialColumns(schema, opts)` | Add `rescuedDataColumn` if needed |
| `cast(value, dataType, ...)` | Type conversion with format support |
| `isSpecial(name, rescued, corrupt)` | Check if column is special |
| `extractWidthsFromMetadata(schema)` | Get widths from `StructField` metadata |

**Type Casting Matrix:**

| Spark Type | Input | Output | On Failure |
|------------|-------|--------|------------|
| `StringType` | Any | String | N/A |
| `IntegerType` | "123" | 123 | NULL |
| `LongType` | "123456789" | 123456789L | NULL |
| `FloatType` | "12.34" | 12.34f | NULL |
| `DoubleType` | "12.34" | 12.34d | NULL |
| `BooleanType` | "true"/"false" | Boolean | NULL |
| `DateType` | "2025-01-15" | Date | NULL |
| `TimestampType` | "2025-01-15 10:30:00" | Timestamp | NULL |
| `DecimalType` | "123.45" | BigDecimal | NULL |

---

## Data Flow

### Read Path

```mermaid
sequenceDiagram
    participant App as Spark Application
    participant DS as DefaultSource
    participant Table as FixedWidthTable
    participant Scan as FixedWidthScan
    participant Hadoop as Hadoop FileSystem
    participant Reader as PartitionReader
    participant Utils as FWUtils

    App->>DS: spark.read.format("fixedwidth-custom-scala")
    DS->>DS: inferSchema() or use provided schema
    DS->>Utils: appendSpecialColumns()
    DS->>Table: getTable(resolvedSchema)
    Table->>Scan: newScanBuilder()

    Note over Scan: Partition Planning
    Scan->>Hadoop: globStatus(path)
    Hadoop-->>Scan: FileStatus[]
    Scan->>Scan: Calculate byte ranges
    Scan-->>App: InputPartition[]

    Note over Reader: Parallel Execution
    App->>Reader: createReader(partition)
    Reader->>Hadoop: open(path)
    Reader->>Hadoop: seek(startByte)

    loop For each line
        Reader->>Reader: readLine()
        Reader->>Utils: extractFields()
        Reader->>Utils: cast() per field
        Reader-->>App: InternalRow
    end

    Reader->>Hadoop: close()
```

### Write Path

```mermaid
sequenceDiagram
    participant App as Spark Application
    participant Table as FixedWidthTable
    participant Writer as FixedWidthWriteBuilder
    participant Committer as DataWritingSparkTask
    participant DataWriter as FixedWidthDataWriter
    participant Hadoop as Hadoop FileSystem

    App->>Table: df.write.format(...).save(path)
    Table->>Writer: newWriteBuilder(info)
    Writer->>Writer: build() → BatchWrite
    Writer->>Committer: createBatchWriterFactory()

    Note over Committer: Parallel Writing
    Committer->>DataWriter: create(partitionId)
    DataWriter->>Hadoop: Create output file
    Note over DataWriter: Config: paddingChar, alignment,<br/>lineEnding, dateFormat,<br/>timestampFormat, timeZone

    loop For each row
        DataWriter->>DataWriter: formatValue() per field
        DataWriter->>DataWriter: Pad to width (alignment + paddingChar)
        DataWriter->>Hadoop: Write line + lineEnding
    end

    DataWriter->>Hadoop: commit()
```

---

## Deployment & Infrastructure

### Supported Environments

| Environment | Support Level | Notes |
|-------------|---------------|-------|
| **Databricks** | ✅ Full | Spark 4.0+, cluster library attachment |
| **Local PySpark** | ✅ Full | JAR via `--jars` or `spark.jars` |
| **Azure Synapse** | ✅ Compatible | Spark 4.0 pools |
| **AWS EMR** | ✅ Compatible | EMR 7.0+ (Spark 4.0) |
| **Google Dataproc** | ✅ Compatible | Dataproc 3.0+ |
| **Self-hosted** | ✅ Compatible | Any Spark 4.0.x cluster |

### Deployment Architecture

```mermaid
flowchart TB
    subgraph Build["Build Environment"]
        SBT["sbt package"]
        JAR["spark-fixedwidth-datasource_2.13-0.1.0-SNAPSHOT.jar"]
    end

    subgraph Deploy["Deployment Options"]
        subgraph Databricks["Databricks"]
            DBLib["Cluster Library"]
            DBFS["DBFS Upload"]
        end
        subgraph Local["Local/Standalone"]
            SparkSubmit["spark-submit --jars"]
            SparkConf["spark.jars config"]
        end
        subgraph Cloud["Cloud Clusters"]
            S3["S3/ADLS/GCS"]
            Init["Init Script"]
        end
    end

    SBT --> JAR
    JAR --> DBLib
    JAR --> DBFS
    JAR --> SparkSubmit
    JAR --> SparkConf
    JAR --> S3
    S3 --> Init
```

### Dependencies

```
spark-fixedwidth-datasource_2.13
├── org.apache.spark:spark-sql_2.13:4.0.0 (provided)
│   ├── org.apache.spark:spark-core_2.13
│   ├── org.apache.spark:spark-catalyst_2.13
│   └── org.apache.hadoop:hadoop-client
└── com.fasterxml.jackson.core:jackson-databind (transitive)
```

---

## Security Considerations

### Data Security

| Aspect | Implementation |
|--------|----------------|
| **File Access** | Inherits Hadoop FileSystem security (HDFS ACLs, S3 IAM, ADLS RBAC) |
| **Credentials** | No credential storage; uses Spark/Hadoop configuration |
| **Data in Transit** | Uses underlying filesystem encryption (HTTPS for cloud storage) |
| **Data at Rest** | Supports reading encrypted files via Hadoop transparent encryption |

### Input Validation

| Validation | Location | Protection |
|------------|----------|------------|
| Option parsing | `FWUtils.parsePositions()` | Malformed `field_lengths` rejected |
| Schema validation | `getTable()` | Invalid schemas rejected |
| File path validation | `FixedWidthScan` | Glob expansion via Hadoop API |
| Type conversion | `FWUtils.cast()` | Safe parsing with NULL fallback |

### Audit & Compliance

- **File Path Tracking**: `_file_path` included in rescued data JSON
- **Error Tracking**: Malformed data captured in `_corrupt_record` or `_rescued_data`
- **No PII Handling**: Library is data-agnostic; compliance is application responsibility

---

## Scalability & Performance

### Performance Characteristics

| Metric | Characteristic |
|--------|----------------|
| **Time Complexity** | O(n) where n = total bytes across files |
| **Space Complexity** | O(partition size) per executor |
| **Parallelism** | Linear scaling with partition count |
| **I/O Pattern** | Sequential reads with byte-range seeking |

### Scalability Features

| Feature | Implementation | Benefit |
|---------|----------------|---------|
| **Byte-Based Partitioning** | Files split by byte offset | True parallel I/O |
| **Lazy Evaluation** | Iterator-based reader | Memory bounded |
| **Glob Expansion** | Multi-file support | Batch processing |
| **Compression Support** | Codec auto-detection | Storage efficiency |

### Optimization Recommendations

```
File Size → Partition Strategy
─────────────────────────────
< 128MB   : Single partition (overhead > benefit)
128MB-1GB : Default auto-partitioning (128MB splits)
> 1GB     : Consider numPartitions for control
> 10GB    : Ensure cluster has adequate executors

maxPartitionBytes tuning:
- Increase: Reduce task overhead, need more memory
- Decrease: Better parallelism, more task scheduling overhead
```

---

## Known Limitations

### Current Bottlenecks

| Limitation | Impact | Workaround |
|------------|--------|------------|
| **Compressed files cannot be split** | Single partition for .gz files | Use uncompressed or splittable codecs (bzip2 is not; lz4 is) |
| **No push-down predicates** | Full file scan always | Pre-filter at storage level |
| **Schema evolution** | Schema must match file structure | Version schemas explicitly |

### Technical Debt Areas

| Area | Issue | Priority |
|------|-------|----------|
| **CRLF byte counting** | `readNextLine()` uses `+1` for newline byte; should use actual byte length for CRLF accuracy | Low |
| **Metadata columns** | `_file_path` only in rescued JSON | Medium |
| **Statistics** | No file-level statistics collection | Low |

---

## Future Improvements

### Planned Architecture Changes

| Enhancement | Description | Complexity |
|-------------|-------------|------------|
| **Column Pruning** | Read only requested columns | Medium |
| **Predicate Pushdown** | Filter rows during parsing | High |
| **Schema Evolution** | Handle changing file formats | Medium |
| **Streaming Support** | `SupportsStreaming` interface | High |

### Migration Strategies

**Upgrading from DataSource V1:**
```
V1 API (deprecated)           V2 API (current)
────────────────────────────────────────────────
RelationProvider      →       TableProvider
BaseRelation          →       Table
InputFormat           →       PartitionReader
OutputFormat          →       DataWriter
```

**Spark Version Compatibility:**
```
Spark Version    Support
─────────────────────────
3.x              Not compatible (V2 API differences)
4.0.x            ✅ Full support
4.1.x            Expected compatible (test when released)
```

---

## Component Dependency Graph

```mermaid
flowchart TD
    subgraph External["External Dependencies"]
        Spark["Apache Spark 4.0"]
        Hadoop["Hadoop 3.x"]
        Jackson["Jackson JSON"]
    end

    subgraph Core["Core Components"]
        DS["DefaultSource"]
        DSImpl["FixedWidthDataSource"]
        Table["FixedWidthTable"]
        Scan["FixedWidthScan"]
        Part["FixedWidthPartition"]
        Factory["PartitionReaderFactory"]
        Reader["PartitionReader"]
        Utils["FWUtils"]
    end

    subgraph Write["Write Components"]
        WriteBuilder["FixedWidthWriteBuilder"]
        BatchWrite["FixedWidthBatchWrite"]
        DataWriter["FixedWidthDataWriter"]
    end

    DS --> DSImpl
    DSImpl --> Table
    DSImpl --> Utils
    Table --> Scan
    Table --> WriteBuilder
    Scan --> Part
    Scan --> Factory
    Factory --> Reader
    Reader --> Utils
    Reader --> Hadoop
    Reader --> Jackson
    WriteBuilder --> BatchWrite
    BatchWrite --> DataWriter
    DataWriter --> Hadoop

    Spark -.-> DS
    Spark -.-> Table
    Spark -.-> Scan
```

---

## References

- [Spark DataSource V2 Guide](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [Spark Connector Development](https://spark.apache.org/docs/latest/sql-data-sources-developer-guide.html)
- [Hadoop FileSystem API](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html)
- [Project Feature Roadmap](FEATURE_ROADMAP.md)
- [API Reference](API_REFERENCE.md)
- [Configuration Guide](CONFIGURATION.md)
