package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, current_timestamp, date_format}

object Test9 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameDateOperations")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample DataFrame
    val data = Seq(
      (1, "2023-09-11 10:30:00"),
      (2, "2023-09-12 14:45:00"),
      (3, "2023-09-13 08:15:00")
    )
    val df = spark.createDataFrame(data).toDF("id", "timestamp")

    // Example 1: Adding a column with full day of the week
    val dfWithWeekDay = df
      .withColumn("week_day_full", date_format(current_timestamp(), "EEEE"))

    // Display the DataFrame with the new column
    dfWithWeekDay.show()

    // Additional Examples:
    // Example 2: Extracting the day of the week from a timestamp column
    val dfExtractedDay = df
      .withColumn("day_of_week", date_format(df("timestamp"), "EEEE"))
    // Same result as above
    val dfExtractedDay1 = df
      .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))

    // Example 3: Extracting the year from a timestamp column
    val dfExtractedYear = df
      .withColumn("year", date_format(df("timestamp"), "yyyy"))

    // Example 4: Adding a new column with a formatted date
    val dfFormattedDate = df
      .withColumn("formatted_date", date_format(df("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    // Show the results of the additional examples
    dfExtractedDay.show()
    dfExtractedDay1.show()
    dfExtractedYear.show()
    dfFormattedDate.show()

    // Stop the SparkSession
    spark.stop()

  }
}
