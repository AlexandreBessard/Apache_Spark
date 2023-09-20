package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object Test27 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for itemsDf
    val data = Seq(
      (1, "Apple"),
      (2, "Banana,Orange"),
      (3, "Grapes,Cherry"),
      (4, "Strawberry")
    )

    // Define the schema
    val schema = Seq(
      "itemId", "itemName"
    )

    // Create a DataFrame
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    itemsDf.show()

    // Define a UDF to split the "itemName" column into an array
    val splitString = udf((input: String) => input.split(",").map(_.trim))

    // Use withColumn to split the "itemName" column into "itemNameElements"
    val resultDf = itemsDf
      .withColumn("itemNameElements", splitString(col("itemName")))

    // Display the DataFrame with the split elements
    println("DataFrame with split elements:")
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
