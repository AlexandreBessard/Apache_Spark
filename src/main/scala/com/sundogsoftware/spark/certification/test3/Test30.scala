  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, date_add}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


  object Test30 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrames
      val data1 = Seq(
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol")
      )

      val data2 = Seq(
        (1, "HR"),
        (2, "Engineering"),
        (4, "Finance")
      )

      val columns1 = Seq("id", "name")
      val columns2 = Seq("id", "department")

      // Create DataFrames
      val df1 = spark.createDataFrame(data1).toDF(columns1: _*)
      val df2 = spark.createDataFrame(data2).toDF(columns2: _*)

      // Perform an inner join between df1 and df2 on the "id" column
      /*
       inner join is a type of join operation that combines two DataFrames or tables
       based on a common column, and it returns only the rows for which there is a match
       in both DataFrames. In other words, it selects and includes only the rows where
       the specified condition is true.
       */
      val resultDF = df1
        .join(df2, df1.col("id") === df2.col("id"), "inner")

      // Show the result
      resultDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
