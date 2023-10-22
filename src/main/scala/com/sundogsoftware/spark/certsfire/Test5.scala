package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sort_array, sqrt}  // Corrected the sqrt import here

object Test5 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data: Let's assume that each item can have multiple attributes.
    val data = Seq(
      (1, "ItemA", Array(3, 1, 2)),
      (2, "ItemB", Array(5, 4)),
      (3, "ItemC", Array(1))
    )

    val itemsDf = data.toDF("itemId", "itemName", "attributes")

    // Display original DataFrame
    println("Original DataFrame:")
    itemsDf.show(false)

    // Sorting array in descending order
    val sortedDf = itemsDf.withColumn("attributes", sort_array($"attributes", asc = false))
    val sortedDf1 = itemsDf.withColumn("attributes", sort_array(col("attributes"), asc = false))
    // DOES NOT COMPILE:
    // val sortedDf2 = itemsDf.withColumn("attributes", sort_array("attributes", asc = false))

    // Display transformed DataFrame
    println("Transformed DataFrame:")
    sortedDf.show(false)
    sortedDf1.show()
    // sortedDf2.show()

    // Closing the SparkSession
    spark.close()
  }
}
