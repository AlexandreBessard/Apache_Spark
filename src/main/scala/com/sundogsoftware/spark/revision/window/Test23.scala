package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size}  // Importing the required 'size' function

object Test23 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: keep element which contains 3 or more element.

    // Sample data: (itemId, itemNameElements)
    val items = List(
      (1, Array("item", "blue")),
      (2, Array("item", "red", "large")),
      (3, Array("item", "green", "medium", "shiny")),
      (4, Array("item"))
    )

    import spark.implicits._
    val itemsDf = items.toDF("itemId", "itemNameElements")

    // Filter rows where the size of "itemNameElements" column is >= 3
    val filteredDf = itemsDf.filter(size($"itemNameElements") >= 3)
    val filteredDf1 = itemsDf.filter(size(col("itemNameElements")) >= 3)
    
    // Display the filtered DataFrame
    filteredDf.show(truncate = false)
    filteredDf1.show(truncate = false)

    // Stop Spark session
    spark.stop()
  }
}
