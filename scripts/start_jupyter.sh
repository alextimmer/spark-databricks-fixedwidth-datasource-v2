#!/bin/bash


# Launch Jupyter Notebook with PySpark + fixed-width datasource ready
set -e

# Set environment variables

# Make sure Python3 is used
export PYSPARK_PYTHON=python3

# Set SPARK_HOME if not already set
export SPARK_HOME=${SPARK_HOME:-/opt/spark-4.0.2-bin-hadoop3}
export PATH=$SPARK_HOME/bin:$PATH
export PATH=$PATH:/usr/local/bin
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Find the fixed-width JAR
FIXEDWIDTH_JAR=$(find "$(pwd)/target/scala-2.13" -name "spark-fixedwidth-datasource_2.13-*.jar" | head -n 1)

# Optional: point to your compiled fixed-width JAR
if [ -n "$FIXEDWIDTH_JAR" ]; then
    echo "Adding fixed-width JAR to PySpark: $FIXEDWIDTH_JAR"
    export PYSPARK_SUBMIT_ARGS="--jars $FIXEDWIDTH_JAR pyspark-shell"
else
    echo "WARNING: Fixed-width JAR not found, Spark will not see the datasource."
fi

# Launch Jupyter Notebook accessible from host
jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root

