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
      Seq(("A", 20), ("B", 30), ("C", 40), ("D", 50), ("E", 60)),
      //numSlices – number of partitions to divide the collection into
      numSlices = 3 // Set the number of partitions to 1 for same partition
    )

    // Create a DataFrame from the RDD
    val df: DataFrame = spark.createDataFrame(dataRDD).toDF("Letter", "Number")

    // Get the number of partitions in the DataFrame's RDD
    val numPartitions = df.rdd.getNumPartitions

    // Print the number of partitions
    println(s"Number of partitions: $numPartitions")

    // Investigating partition contents
    df.rdd.mapPartitionsWithIndex((index, iter) => {
      println(s"Content of partition $index:")
      iter.foreach(x => println(s" - $x"))
      iter
    }).collect() // calling action to trigger the transformation


    // Stop the SparkSession
    spark.stop()

  }
}
