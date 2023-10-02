package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test18 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test18") // Updated to match the object name
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = (1 to 2000).map(i => (i, s"Product$i", i * 0.5))

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF("transactionId", "itemName", "predError")

    // Sample approximately 1000 rows with potential duplicates
    // If duplicate can be returned, set the "withReplacement" argument to true else false without duplicate.
    val sampledDf = transactionsDf.sample(withReplacement = true, fraction = 0.5)

    // Show the sampled DataFrame
    sampledDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
