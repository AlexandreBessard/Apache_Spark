package com.sundogsoftware.spark.revision

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array_contains, col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test8 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()


    // Sample DataFrame (replace this with your actual DataFrame)
    val data = Seq(
      (1, Array("cozy", "spacious", "modern"), "ItemA"),
      (2, Array("rustic", "cozy"), "ItemB"),
      (3, Array("cozy", "vintage"), "ItemC"),
      (4, Array("toto", "vintage"), "ItemC")
    )
    val schema = List("itemId", "attributes", "itemName")
    val itemsDF: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Filter rows where "attributes" array contains "cozy"
    val filteredDF = itemsDF
      .filter(array_contains(col("attributes"), "cozy"))

    filteredDF.show()

    // Select "itemId" and explode "attributes" array
    // Select only 2 columns, itemId and attributes which is renamed to attribute
    val explodedDF = filteredDF
      .select(col("itemId"), explode(col("attributes")).alias("attribute"))

    // Show the resulting DataFrame
    explodedDF.show()

    // Stop the SparkSession
    spark.stop()
  }
}
