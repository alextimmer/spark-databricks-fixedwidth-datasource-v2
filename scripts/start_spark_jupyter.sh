#!/bin/bash
# start_spark_jupyter.sh

# Ensure Python and PySpark are used
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
# Token/password auth disabled — local testing inside the devcontainer only
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root --IdentityProvider.token='' --ServerApp.password=''"

# Start PySpark locally with all cores
pyspark --master local[*] --jars "$(ls ../target/scala-2.13/spark-fixedwidth-datasource_2.13-*.jar | head -n 1)"
