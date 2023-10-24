package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object Test22 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data
    val itemsData = Seq(
      (1, "apple", "Fruit Inc."),
      (2, "banana", "Tropics Corp."),
      (3, "cherry", "Berry Inc."),
      (4, "date", "Desert LLC")
    )

    val itemsDf = itemsData.toDF("itemId", "itemName", "supplier")

    // Define the accumulator
    val accum = spark.sparkContext.longAccumulator("IncSupplierAccumulator")

    // Function to check if "Inc." exists in the supplier name
    def check_if_inc_in_supplier(row: Row): Unit = {
      val supplierName = row.getAs[String]("supplier")
      if (supplierName.contains("Inc.")) {
        accum.add(1)
      }
    }

    // Apply the function on the DataFrame
    itemsDf.rdd.foreach(check_if_inc_in_supplier)

    // Print the accumulator's value
    println(s"Number of suppliers with 'Inc.': ${accum.value}")

    spark.stop()
  }
}
