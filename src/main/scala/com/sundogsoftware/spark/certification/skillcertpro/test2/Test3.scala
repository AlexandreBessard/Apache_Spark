package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, collect_list, sort_array, struct, to_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(("2023-09-09"), ("2023-08-15"), ("2023-07-01"))

    // Create a DataFrame
    import spark.implicits._
    val df = data.toDF("date")

    // Select and convert the date column to a timestamp
    val resultDf = df.withColumn("timestamp", to_timestamp($"date", "yyyy-dd-MM").cast(TimestampType))

    val resultDf1 = df.select(to_timestamp(col("date"), "yyyy-dd-MM").as("timestamp"))

    // Show the result
    resultDf1.show()


    // Stop the SparkSession
    spark.stop()

  }
}
