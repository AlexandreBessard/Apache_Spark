  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

  object Test22 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrame
      val data = Seq(
        ("A1001", 10),
        ("A1002", 20),
        ("A1001", 10),
        ("A1003", 30),
        ("A1002", 20),
        ("A1002", 30)
      )

      val columns = Seq("InvoiceNo", "Quantity")

      // Create a DataFrame
      val df = spark.createDataFrame(data).toDF(columns: _*)

      // Group by "InvoiceNo" and calculate the count of distinct "Quantity" values
      val resultDF = df.groupBy("InvoiceNo")
        .agg(expr("count(distinct Quantity) as DistinctQuantityCount"))

      // Show the result DataFrame
      resultDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
