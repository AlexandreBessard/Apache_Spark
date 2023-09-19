package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test14 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Renamed an existing column and copy the existing DataFrame
     */

    // Sample data for itemsDf
    val data = Seq(
      ("product1", "supplier1"),
      ("product2", "supplier2"),
      ("product3", "supplier3")
    )

    // Define the schema
    val schema = Seq(
      "product", "supplier"
    )

    // Create a DataFrame
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    itemsDf.show()

    // Rename the "supplier" column to "manufacturer"
    val renamedDf = itemsDf.withColumnRenamed("supplier", "manufacturer")

    // Display the DataFrame after renaming
    println("DataFrame after renaming column:")
    renamedDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
