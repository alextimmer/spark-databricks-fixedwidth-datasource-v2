# Contributing Guide

Guidelines for contributing to the Spark Fixed-Width Data Source project.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Environment](#development-environment)
3. [Code Standards](#code-standards)
4. [Testing Requirements](#testing-requirements)
5. [Pull Request Process](#pull-request-process)
6. [Feature Development Workflow](#feature-development-workflow)
7. [Documentation](#documentation)

---

## Getting Started

### Prerequisites

- **Docker Desktop** (for DevContainer, recommended)
- **VS Code** with Dev Containers extension
- OR: JDK 17+, SBT 1.9+, Spark 4.0.2

### Clone Repository

```bash
git clone <repository-url>
cd sparkfixedwidthdatasource-scala
```

### DevContainer Setup (Recommended)

1. Open in VS Code
2. Press `Ctrl+Shift+P` → "Dev Containers: Reopen in Container"
3. Wait for container build
4. Verify setup:
   ```bash
   ./build_and_test.sh
   ```

### Manual Setup

```bash
# Verify Java
java -version  # Should be 17+

# Verify SBT
sbt --version

# Verify Spark binaries
ls /opt/spark-4.0.2-bin-hadoop3/
```

---

## Development Environment

### Project Structure

```
sparkfixedwidthdatasource-scala/
├── src/main/scala/com/alexandertimmer/fixedwidth/  # Source code
│   ├── DefaultSource.scala
│   ├── FixedWidthDataSource.scala
│   ├── FixedWidthTable.scala
│   ├── FixedWidthScan.scala
│   ├── FixedWidthPartition.scala
│   ├── FixedWidthPartitionReader.scala
│   ├── FixedWidthDataWriter.scala
│   ├── FixedWidthBatchWrite.scala
│   ├── FixedWidthWriteBuilder.scala
│   ├── FixedWidthConstants.scala
│   └── FWUtils.scala
├── src/test/scala/com/alexandertimmer/fixedwidth/  # Tests
│   ├── FixedWidthRelationSpec.scala
│   ├── MissingFunctionalitySpec.scala
│   └── RefactoringValidationSpec.scala
├── src/main/resources/META-INF/  # ServiceLoader
├── docs/                         # Documentation
├── data/test_data/               # Test files
└── notebook/                     # Jupyter tests
```

### Build Commands

| Command | Purpose |
|---------|---------|
| `sbt compile` | Compile sources |
| `sbt package` | Build JAR |
| `sbt clean package` | Clean build |
| `sbt test` | Run unit tests |
| `./scripts/build_and_test.sh` | Full build + test |

### Development Workflow

1. Make changes in `src/main/scala/com/alexandertimmer/fixedwidth/`
2. Compile: `sbt compile`
3. Run tests: `sbt test`
4. Package: `sbt package`
5. Test in Jupyter (if needed): `./scripts/start_jupyter.sh`

---

## Code Standards

### Scala Style Guide

We follow the [Databricks Scala Style Guide](https://github.com/databricks/scala-style-guide):

#### Immutability

```scala
// ✅ Good: Use val
val fieldLengths = parsePositions(opts)

// ❌ Bad: Avoid var unless necessary
var counter = 0
```

#### Option Instead of Null

```scala
// ✅ Good: Use Option
val nullValue: Option[String] = Option(opts.get("nullValue"))

// ❌ Bad: Don't use nullable types
val nullValue: String = opts.get("nullValue")  // Could be null
```

#### Pattern Matching

```scala
// ✅ Good: Pattern matching
mode match {
  case "PERMISSIVE" => handlePermissive()
  case "DROPMALFORMED" => handleDrop()
  case "FAILFAST" => handleFail()
  case _ => throw new IllegalArgumentException(s"Unknown mode: $mode")
}

// ❌ Bad: Nested if-else
if (mode == "PERMISSIVE") {
  handlePermissive()
} else if (mode == "DROPMALFORMED") {
  handleDrop()
} // ...
```

#### Explicit Types

```scala
// ✅ Good: Explicit return types on public methods
def parsePositions(opts: CaseInsensitiveStringMap): Array[(Int, Int)] = { ... }

// ❌ Bad: Implicit return types on public methods
def parsePositions(opts: CaseInsensitiveStringMap) = { ... }
```

#### Method Size

- **Maximum 30 lines** per method
- Extract helper methods for complex logic
- Each method should do one thing

#### Class Size

- **Maximum 300 lines** per class
- Split large classes into focused modules

### Formatting

- **2-space indentation** (no tabs)
- **Line length**: ≤ 150 characters
- **Blank line** between method definitions
- **Import ordering**: stdlib, third-party, project

---

## Testing Requirements

### Test Framework

We use **ScalaTest** with `AnyFunSuite`:

```scala
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class FeatureSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  // Tests here
}
```

### Test Requirements

1. **All new features must have tests**
2. **All bug fixes must have regression tests**
3. **Tests must pass before merge**
4. **Cover edge cases and error conditions**

### Test Naming Convention

```scala
// Pattern: "feature: specific scenario description"
test("trimValues=true: whitespace is trimmed from parsed values") { ... }
test("nullValue: empty string becomes null") { ... }
test("compression: read gzip compressed file") { ... }
```

### Test Structure (AAA Pattern)

```scala
test("feature: expected behavior") {
  // ARRANGE
  val testContent = "test data"
  val testFile = createTestFile(testContent)

  // ACT
  val df = spark.read.format("fixedwidth-custom-scala")
    .option("field_lengths", "0:10")
    .load(testFile.getAbsolutePath)
  val results = df.collect()

  // ASSERT
  results.length shouldBe 1
  results(0).getString(0) shouldBe "test data"
}
```

### Test Categories

| Category | Required For |
|----------|--------------|
| **Happy Path** | All features |
| **Default Behavior** | Option-based features |
| **Edge Cases** | All features |
| **Error Handling** | All features |
| **Option Combinations** | Interacting options |

### Running Tests

```bash
# All tests
sbt test

# Specific test pattern
sbt 'testOnly -- -z "trimValues"'

# Specific test class
sbt 'testOnly fixedwidth.FixedWidthRelationSpec'

# With verbose output
sbt 'testOnly -- -oF'
```

---

## Pull Request Process

### Before Submitting

1. **Tests pass**: `sbt test` ✅
2. **No compiler warnings**: `sbt compile` ✅
3. **Code formatted**: Follows style guide
4. **Documentation updated**: If applicable
5. **Commits are atomic**: One logical change per commit

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance

**Examples:**
```
feat(parser): add trimValues option for whitespace handling

Adds trimValues option (default: true) that trims leading and trailing
whitespace from parsed field values.

Closes #123
```

```
fix(reader): handle empty partition correctly

Empty partitions now return early without attempting to read,
fixing ArrayIndexOutOfBoundsException on empty splits.
```

### Pull Request Template

```markdown
## Description
Brief description of changes.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] All tests pass locally
- [ ] Tested in Jupyter notebook (if applicable)

## Checklist
- [ ] Code follows style guide
- [ ] Documentation updated
- [ ] Commit messages follow convention
```

### Review Process

1. Create PR against `main` branch
2. Automated tests run via CI
3. At least one approving review required
4. Squash and merge (for feature branches)

---

## Feature Development Workflow

### TDD Approach (Recommended)

We follow Test-Driven Development:

```
RED → GREEN → REFACTOR → VERIFY
```

#### Step 1: RED - Write Failing Test

```scala
test("newOption=true: expected behavior") {
  val df = spark.read.format("fixedwidth-custom-scala")
    .option("field_lengths", "0:10")
    .option("newOption", "true")
    .load(testFile.getAbsolutePath)

  val results = df.collect()
  results(0).getString(0) shouldBe "expected"
}
```

#### Step 2: GREEN - Minimal Implementation

```scala
// Add option handling
val newOption = Option(options.get("newOption")).map(_.toBoolean).getOrElse(false)

// Implement feature
if (newOption) {
  // minimal code to pass test
}
```

#### Step 3: REFACTOR - Clean Up

- Extract helper methods
- Improve naming
- Remove duplication
- Add documentation

#### Step 4: VERIFY - Run Full Suite

```bash
sbt test
# All 54 tests should pass
```

### Feature Roadmap Reference

For planned features, see [FEATURE_ROADMAP.md](docs/FEATURE_ROADMAP.md):

1. Check feature priority and complexity
2. Review implementation notes
3. Check test matrix for expected behaviors
4. Implement following TDD workflow

---

## Documentation

### Required Documentation

| Change Type | Documentation Required |
|-------------|------------------------|
| New option | README.md, API_REFERENCE.md, CONFIGURATION.md |
| New feature | README.md, Architecture if significant |
| Bug fix | None (unless behavior change) |
| Breaking change | README.md, migration notes |

### Documentation Standards

- **Clear examples** for all options
- **Tables** for option references
- **Code blocks** with syntax highlighting
- **Links** between related docs

### Building Documentation

```bash
# Preview documentation (if using mkdocs or similar)
# Currently: Markdown files in /docs/
```

---

## Questions?

- Check [Troubleshooting Guide](docs/TROUBLESHOOTING.md)
- Review existing issues
- Open a new issue with details

---

## License

By contributing, you agree that your contributions will be licensed under the project's license.
