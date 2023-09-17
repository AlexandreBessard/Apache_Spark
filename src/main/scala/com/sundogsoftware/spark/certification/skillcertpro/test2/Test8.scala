package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test8 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data as an RDD with a single partition
    val dataRDD: RDD[(String, Int)] = spark.sparkContext.parallelize(
      Seq(("A", 20), ("B", 30), ("C", 40)),
      numSlices = 2 // Set the number of partitions to 1 for same partition
    )

    // Create a DataFrame from the RDD
    val df: DataFrame = spark.createDataFrame(dataRDD).toDF("Letter", "Number")

    // Get the number of partitions in the DataFrame's RDD
    val numPartitions = df.rdd.getNumPartitions

    // Print the number of partitions
    println(s"Number of partitions: $numPartitions")


    // Stop the SparkSession
    spark.stop()

  }
}
