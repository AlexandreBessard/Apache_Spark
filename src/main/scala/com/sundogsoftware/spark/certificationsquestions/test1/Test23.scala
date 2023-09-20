package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}

object Test23 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Create new DataFrame with 2 new columns, one colum is type string and the other one is double
     */

    // Sample data
    val data = Seq(
      ("Spring", 12.5),
      ("Summer", 9.8),
      ("Fall", 8.2),
      ("Winter", 11.0)
    )

    // Define the schema for the new DataFrame
    val schema = StructType(Seq(
      StructField("season", StringType, false), // String column
      StructField("wind_speed_ms", DoubleType, false) // Double column
    ))

    // Create a DataFrame with the specified schema and data
    val newDf: DataFrame = spark.createDataFrame(data).toDF(schema.fieldNames: _*)

    // Show the new DataFrame
    newDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
