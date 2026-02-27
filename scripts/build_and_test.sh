#!/bin/bash
set -e

echo "Cleaning previous builds..."
sbt clean

echo "Compiling project..."
sbt compile

echo "Running tests..."
sbt test

echo "Packaging jar..."
sbt package

echo "Done! Jar is in target/scala-2.13/"
