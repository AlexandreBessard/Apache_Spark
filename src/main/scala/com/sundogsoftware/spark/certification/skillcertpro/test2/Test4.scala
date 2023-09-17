package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit, to_timestamp}
import org.apache.spark.sql.types.TimestampType

object Test4 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    val rawData = Seq(("A", "20"), ("B", "30"), ("C", "80"))

    // Create a DataFrame
    val df: DataFrame = spark.createDataFrame(rawData).toDF("Letter", "Number")

    // Create a new column "Row" by concatenating "Letter" and "Number" columns
    val formattedDf = df.withColumn("Row", expr("concat(Letter, ' ', Number)"))

    formattedDf.show()

    // Select only the "Row" column and write it to a text file
    formattedDf.select("Row").write.text("my_file.txt")



    // Stop the SparkSession
    spark.stop()

  }
}
