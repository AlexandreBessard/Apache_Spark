package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for DataFrame d1
    val data1 = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    )
    val schema1 = Seq("id", "name")
    val d1: DataFrame = spark.createDataFrame(data1).toDF(schema1: _*)

    // Sample data for DataFrame d2
    val data2 = Seq(
      (1, "Manager"),
      (2, "Developer"),
      (4, "Designer") // Note that there is no id=3 in this DataFrame
    )
    val schema2 = Seq("id", "role")
    val d2: DataFrame = spark.createDataFrame(data2).toDF(schema2: _*)

    // Inner join based on the "id" column
    val result: DataFrame = d1.join(d2, d1.col("id") === d2.col("id"), "inner")

    // Show the result
    result.show()

    // Stop the SparkSession
    spark.stop()

  }
}
