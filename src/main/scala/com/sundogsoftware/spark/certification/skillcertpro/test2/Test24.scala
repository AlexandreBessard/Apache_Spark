  package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test24 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("Alice", 28),
      ("Bob", 32),
      ("Charlie", 25)
    )

    // Define the schema
    val schema = List("Name", "Age")

    // Create a DataFrame
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Register the DataFrame as a global temporary view
    /*
    A global temporary view in Apache Spark is a named and queryable view that
    allows you to share a DataFrame across multiple Spark sessions. It is temporary
    in the sense that it exists for the duration of your Spark application but is not
    tied to a specific Spark session.
     */
    df.createOrReplaceGlobalTempView("my_global_view")

    // Read the global temporary view
    val globalTempViewDF = spark.read.table("global_temp.my_global_view")

    // Show the data from the global temporary view
    println("Data from the global temporary view:")
    globalTempViewDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}