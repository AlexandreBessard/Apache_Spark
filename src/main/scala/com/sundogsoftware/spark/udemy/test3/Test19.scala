package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, explode, slice, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test19 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "Thick Coat for Walking in the Snow", Array("blue", "winter", "cozy"), "Sports Company Inc."),
      (2, "Elegant Outdoors Summer Dress", Array("red", "summer", "fresh", "cooling"), "YetiX"),
      (3, "Outdoors Backpack", Array("green", "summer", "travel"), "Sports Company Inc.")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "attributes", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Filter rows where the supplier name includes "Sports"
    val filteredDf = itemsDf
      .filter(col("supplier").contains("Sports"))

    filteredDf.show()

    // Explode the 'attributes' array into separate rows
    val explodedDf = filteredDf
      .withColumn("attribute", explode(col("attributes")))

    explodedDf.show()

    // Select and rename the columns for the final DataFrame
    val resultDf = explodedDf
      .select("itemName", "attribute")
      .withColumnRenamed("attribute", "selectedAttribute")

    // Show the resulting DataFrame
    resultDf.show(truncate = false)


    // Stop the SparkSession
    spark.stop()
  }
}
