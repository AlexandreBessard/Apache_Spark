package com.sundogsoftware.spark.revision

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test14 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    import spark.implicits._

    // Sample DataFrame (you can replace this with your actual DataFrame)
    val transactionsDf: DataFrame = spark.createDataFrame(Seq(
      (1, 101, "2023-09-26"),
      (2, 102, "2023-09-27"),
      (3, 101, "2023-09-28"),
      // Add more data here...
    )).toDF("transactionId", "storeId", "transactionDate")

    // Repartition the DataFrame by "storeId" and "transactionDate" into 14 partitions
    val repartitionedDf = transactionsDf.repartition(14, col("storeId"), col("transactionDate"))

    // Count the number of rows in each partition
    val partitionCounts = repartitionedDf.rdd.mapPartitionsWithIndex {
      (index, iterator) => Iterator((index, iterator.size))
    }.collect()

    // Show the partition counts
    partitionCounts.foreach {
      case (partitionIndex, rowCount) =>
        println(s"Partition $partitionIndex contains $rowCount rows")
    }

    // Stop the SparkSession
    spark.stop()
  }
}
