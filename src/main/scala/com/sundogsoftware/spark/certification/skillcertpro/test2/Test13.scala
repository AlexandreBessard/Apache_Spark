package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test13 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // Create a DataFrame from the sample data
    import spark.implicits._
    val df: DataFrame = data.toDF("value")

    // Check the initial number of partitions
    val initialPartitions = df.rdd.getNumPartitions
    println(s"Initial number of partitions: $initialPartitions")

    // Reduce the number of partitions to 4 using coalesce
    val coalescedDf: DataFrame = df.coalesce(4)

    // Check the final number of partitions
    val finalPartitions = coalescedDf.rdd.getNumPartitions
    println(s"Final number of partitions after coalesce: $finalPartitions")

    // Stop the SparkSession
    spark.stop()

  }
}
