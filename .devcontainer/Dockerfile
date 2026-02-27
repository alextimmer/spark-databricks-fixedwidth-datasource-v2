# ---------------------------
# Base
# ---------------------------
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.2_2.13.18

USER root

# ---------------------------
# Install utilities & Python
# ---------------------------
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    git \
    unzip \
    bash \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------
# Install Spark 4.0.2 Hadoop 3
# ---------------------------
RUN curl -fLo spark.tgz https://downloads.apache.org/spark/spark-4.0.2/spark-4.0.2-bin-hadoop3.tgz \
    && tar -xzf spark.tgz -C /opt \
    && rm spark.tgz

ENV SPARK_HOME=/opt/spark-4.0.2-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# ---------------------------
# Install PySpark and  Notebook for local testing (optional)
# ---------------------------
RUN pip3 install --break-system-packages pyspark==4.0.2 notebook

ENV PATH=$PATH:/usr/local/bin
# ---------------------------
# Set workdir
# ---------------------------
RUN mkdir -p /workspace
WORKDIR /workspace

# ---------------------------
# Copy helper scripts for jupyter
# ---------------------------

# Copy your helper scripts into the container
#COPY start_jupyter.sh /workspace/start_jupyter.sh

# Make it executable
#RUN chmod +x /workspace/start_jupyter.sh

# ---------------------------
# Set alias for convenience
# ---------------------------
RUN echo "alias jupyter-spark='/workspace/start_jupyter.sh'" >> /root/.bashrc

# ---------------------------
# nbextmake Spark + PySpark fully visible in all Python shells
# ---------------------------
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# ---------------------------
# Recommended: sbt & VSCode
# ---------------------------
# VS Code will mount the workspace here automatically
