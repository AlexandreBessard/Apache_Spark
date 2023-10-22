package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, size}

object Test3 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "ItemA", "John Doe Supplies"),
      (2, "ItemB", "Global Imports"),
      (3, "ItemC", "Smith and Sons"),
      (4, "ItemD", "ACME")
    )

    val itemsDf = data.toDF("itemId", "itemName", "supplier")

    // Counting the number of words in the "supplier" column
    val supplierWordCountDf =
      itemsDf.select($"itemId", $"itemName", size(split($"supplier", " ")).alias("supplier_word_count"))

    supplierWordCountDf.show()

    // Closing the SparkSession
    spark.close()
  }
}
