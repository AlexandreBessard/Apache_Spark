package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test8 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    // Set the number of shuffle partitions to 20
    spark.conf.set("spark.sql.shuffle.partitions", 20)

    // Now, when you perform operations that involve shuffling, Spark will use 20 partitions.
    
    // Stop the SparkSession
    spark.stop()
  }
}
