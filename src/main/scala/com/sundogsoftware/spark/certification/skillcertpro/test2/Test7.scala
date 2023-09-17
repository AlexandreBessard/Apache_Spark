package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test7 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(("A", 20), ("B", 30), ("C", 40), ("A", 20))

    // Create a DataFrame
    val df: DataFrame = spark.createDataFrame(data).toDF("Letter", "Number")

    // Add a new column "new_column" with a constant value of 1
    val resultDf = df.withColumn("new_column", lit(1))

    // Show the result
    resultDf.show()

    // Get the number of partitions in the underlying RDD
    val numPartitions = df.rdd.getNumPartitions

    // Print the number of partitions
    println(s"Number of partitions: $numPartitions")

    // Stop the SparkSession
    spark.stop()

  }
}
