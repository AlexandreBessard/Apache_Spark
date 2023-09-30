package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, date_add, to_date}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test6 {

  // Create case class with schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine, named RatingsCounter
    val spark = SparkSession
      .builder
      .appName("RatingsCounter")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      Row("red"),
      Row("yellow"),
      Row("blue")
    )

    // Define the schema with a single "today" column
    val schema = StructType(Seq(StructField("color", StringType, true)))

    // Create a DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Print results from the dataset
    df.show()
  }
}
