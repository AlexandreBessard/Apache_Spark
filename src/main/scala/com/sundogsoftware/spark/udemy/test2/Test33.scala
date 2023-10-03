package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test33 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test29")
      .master("local[*]")
      .getOrCreate()

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

    // Define a UDF (User-Defined Function)
    val add2IfGeq3: Double => Double = {
      case x if x == null => x
      // if x >= 3 add + 2 to x
      case x if x >= 3.0 => x + 2.0
      case x => x
    }

    // Register the UDF with Spark
    // By default, the return type of UDF if not mentionned is a StringType
    val add2IfGeq3Udf: UserDefinedFunction = udf(add2IfGeq3)

    // Add a new column "predErrorAdded" to the DataFrame using the UDF
    val resultDf = transactionsDf.withColumn("predErrorAdded", add2IfGeq3Udf(col("predError")))

    // Show the resulting DataFrame
    resultDf.show()


    // Stop the SparkSession
    spark.stop()
  }
}
