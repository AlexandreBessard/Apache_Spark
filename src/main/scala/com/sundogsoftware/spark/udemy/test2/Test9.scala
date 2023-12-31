package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test9 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test9") // Updated to match the object name
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", 3.0),
      (2, "ProductB", 2.5),
      (3, "ProductA", 1.8),
      (4, "ProductC", 4.2),
      (5, "ProductB", 3.7)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "predError")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Sample 15% of rows with replacement and calculate the average prediction error
    val sampledDf = transactionsDf
      .sample(withReplacement = true, fraction = 0.20)
      .select(avg(col("predError")))

    val sampledDf1 = transactionsDf
      .sample(withReplacement = true, fraction = 0.20)

    /*
    +--------------+
    |avg(predError)|
    +--------------+
    |          2.75|
    +--------------+
     */

    sampledDf.show()
    sampledDf1.show()

    // Stop the SparkSession
    spark.stop()
  }
}
