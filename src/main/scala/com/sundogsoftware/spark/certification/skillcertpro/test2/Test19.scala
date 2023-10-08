package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Test19 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("HR", "Alice", 90),
      ("HR", "Charlie", 88),
      ("IT", "Bob", 92),
      ("HR", "David", 85),
      ("IT", "Eve", 87),
      ("IT", "Frank", 85)
    )

    /*
    Now, you want to calculate the rank of each employee within
    their department based on their score, and you also want to order the
    employees within each department by their score in descending order.
    You can achieve this using the windowSpec as follows:
     */
    // Define the schema
    val schema = List("department", "name", "score")

    // Create the DataFrame
    import spark.implicits._
    val employeeDF = data.toDF(schema: _*)

    // Define a Window specification
    val windowSpec = Window.partitionBy("department")
      .orderBy(desc("score"))

    // Calculate the rank of employees within their department based on score
    val rankedDF = employeeDF.withColumn("rank", dense_rank().over(windowSpec))

    // Show the final result
    rankedDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}
