  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.asc_nulls_last
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Create a DataFrame with sample data
    val data = Seq(
      ("Alice", 30),
      ("Bob", 25),
      ("Charlie", 35),
      ("David", 28),
      ("Eve", 40)
    )

    val columns = Seq("Name", "Age")

    import spark.implicits._
    val df = data.toDF(columns: _*)

    // Use the take() method to retrieve the first 3 rows from the DataFrame
    /*
    The take() method in Apache Spark is used to retrieve a specified number of elements from a DataFrame or RDD
    (Resilient Distributed Dataset) and return them as an array or list in the driver program.
    This method can be helpful when you want to preview a small subset of your data or extract a few records for further analysis.
     */
    val takenRows = df.take(3)

    // Display the result
    takenRows.foreach(println)

    // Stop the SparkSession
    spark.stop()

  }
}