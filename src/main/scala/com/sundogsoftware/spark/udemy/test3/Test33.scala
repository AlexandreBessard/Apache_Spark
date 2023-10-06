package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test33 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data as a sequence of Float values
    val throughputRates: Seq[Float] = Seq(0.1f, 0.2f, 0.3f, 0.4f, 0.5f)

    // Import implicits to use toDF
    import spark.implicits._

    // Convert the sequence to a DataFrame with FloatType
    val throughputRatesDf: DataFrame = throughputRates.toDF("throughputRate")

    // Show the resulting DataFrame
    throughputRatesDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
