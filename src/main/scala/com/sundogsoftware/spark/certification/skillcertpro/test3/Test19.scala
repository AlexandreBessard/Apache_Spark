  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

  object Test19 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Unpersist a table name: "my_table"

      // Register a temporary table from a DataFrame
      val data = Seq(
        (1, "John", 30),
        (2, "Jane", 25),
        (3, "Bob", 35)
      )

      val columns = Seq("id", "name", "age")

      val df = spark.createDataFrame(data).toDF(columns: _*)
      df.createOrReplaceTempView("my_table")

      // Cache the table in memory
      spark.catalog.cacheTable("my_table")

      // Perform some operations on the cached table
      val result = spark.sql("SELECT * FROM my_table WHERE age > 30")
      result.show()

      // Uncache the table to free up memory
      spark.catalog.uncacheTable("my_table")

      // Stop the SparkSession
      spark.stop()

    }
  }
