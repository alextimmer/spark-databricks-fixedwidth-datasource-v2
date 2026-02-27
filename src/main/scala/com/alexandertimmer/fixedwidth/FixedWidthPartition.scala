// SPDX-License-Identifier: Apache-2.0
package com.alexandertimmer.fixedwidth

import org.apache.spark.sql.connector.read.InputPartition

/**
 * Represents a partition of a fixed-width file for parallel reading.
 *
 * @param path The file path to read
 * @param start Byte offset to start reading from (0 for first partition)
 * @param length Number of bytes this partition is responsible for
 * @param isFirstSplit True if this is the first partition (no partial line to skip)
 */
case class FixedWidthPartition(
    path: String,
    start: Long = 0,
    length: Long = Long.MaxValue,
    isFirstSplit: Boolean = true
) extends InputPartition
