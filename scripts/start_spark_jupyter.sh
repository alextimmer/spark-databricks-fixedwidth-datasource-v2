#!/bin/bash
# start_spark_jupyter.sh

# Ensure Python and PySpark are used
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
# Token/password auth disabled — local testing inside the devcontainer only
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root --IdentityProvider.token='' --ServerApp.password=''"

# Start PySpark locally with all cores.
# JAR resolved relative to the repo root (newest one), so the script works
# no matter which directory it is launched from.
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXEDWIDTH_JAR=$(ls -t "$REPO_ROOT"/target/scala-2.13/spark-fixedwidth-datasource_2.13-*.jar 2>/dev/null | head -n 1)
if [ -z "$FIXEDWIDTH_JAR" ]; then
    echo "ERROR: fixed-width JAR not found — run 'sbt package' first." >&2
    exit 1
fi
echo "Using JAR: $FIXEDWIDTH_JAR"
pyspark --master local[*] --jars "$FIXEDWIDTH_JAR"
