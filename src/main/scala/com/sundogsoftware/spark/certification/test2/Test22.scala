  package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


  object Test22 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("1001", "ProductA", 5),
      ("1001", "ProductB", 3),
      ("1002", "ProductC", 2),
      ("1003", "ProductD", 4),
      ("1003", "ProductE", 2)
    )

    // Define the schema
    val schema = List("InvoiceNo", "Description", "Quantity")

    // Create the DataFrame
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Group by "InvoiceNo" and calculate the count of "Quantity" for each group
    val resultDF = df.groupBy("InvoiceNo")
      //The count of quantity
      .agg(expr("count(Quantity)").alias("TotalQuantity"))

    // Group by "InvoiceNo" and sum the "Quantity" for each group
    val resultDF1 = df.groupBy("InvoiceNo")
      .agg(sum("Quantity").alias("TotalQuantity"))

    // Show the result
    resultDF.show()

    resultDF1.show()

    // Stop the SparkSession
    spark.stop()

  }
}