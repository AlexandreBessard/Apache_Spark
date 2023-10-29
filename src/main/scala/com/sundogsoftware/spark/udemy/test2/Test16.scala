package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}  // Add this import

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test16 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test16")
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // Create a sequence of timestamp values
    val dateValues = Seq(
      java.sql.Timestamp.valueOf("2023-09-26 10:00:00"), // timestamp row, easy to apply the correct format
      java.sql.Timestamp.valueOf("2023-09-27 11:30:00")
    )


    // Create a DataFrame from the date values and schema
    val dfDates: DataFrame = spark.createDataFrame(dateValues.map(Tuple1.apply)).toDF("date")

    dfDates.printSchema()

    // Convert the "date" column to a timestamp using to_timestamp
    // to_timestamp -> convert string to timestamp. the format must be exactly the same as the string to be converted properly.
    val formattedDf = dfDates
      .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

    // Show the resulting DataFrame
    formattedDf.show()

    formattedDf.printSchema()

    // Stop the SparkSession
    spark.stop()
  }
}
