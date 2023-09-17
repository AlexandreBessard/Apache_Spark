  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


  object Test27 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrame
      val data = Seq(
        ("Alice", "HR", 5000),
        ("Bob", "Engineering", 6000),
        ("Carol", "HR", 5500),
        ("David", "Engineering", 7000),
        ("Eve", "Finance", 6500)
      )

      val columns = Seq("EmployeeName", "Department", "Salary")

      // Create a DataFrame
      val df = spark.createDataFrame(data).toDF(columns: _*)

      // Group by "Department" and calculate the sum of "Salary" for each department
      val resultDF = df.groupBy("Department").sum("Salary")

      // Show the result
      resultDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
