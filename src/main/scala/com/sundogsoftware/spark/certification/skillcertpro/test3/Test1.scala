  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc_nulls_last, desc_nulls_first}


  object Test1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Create a sample DataFrame
    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, null),
      (4, "Eve"),
      (5, "Charlie"),
      (6, null)
    )

    val columns = Seq("a", "b")

    val df: DataFrame = spark.createDataFrame(data).toDF(columns: _*)

    // Order the DataFrame in ascending order with null values at the end
    // Null value are placed at the end contained in the column "a"
    val orderedDf = df.orderBy(asc_nulls_last("a")) // wrong column name

    val orderedDf1 = df.orderBy(asc_nulls_last("b"))

    orderedDf.show()
    orderedDf1.show()

    // Stop the SparkSession
    spark.stop()

  }
}