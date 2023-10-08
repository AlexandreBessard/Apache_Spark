  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


  object Test16 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrame
      val rawData = Seq(
        (1, "HR", 5000),
        (2, "IT", 6000),
        (3, "IT", 5500),
        (4, "HR", 4800),
        (5, "Finance", 7000),
        (6, "Finance", 7200)
      )

      // Define the schema
      val schema = List("employee_id", "department", "salary")
      val df: DataFrame = spark.createDataFrame(rawData).toDF(schema: _*)

      // Define the window specification
      // ascending order by default
      val windowSpec = Window.partitionBy("department").orderBy("salary")

      // Add a dense rank column based on the window specification.
      // dense_rank -> return the ranks of rows within a partition.
      val resultDF = df.withColumn("dense_rank", dense_rank().over(windowSpec))

      // Show the DataFrame with the dense rank column
      resultDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
