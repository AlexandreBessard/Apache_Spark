package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test25 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Read parquet file at location into a DataFrame.
     */

    // Define the schema for the Parquet file
    val fileSchema = StructType(Seq(
      StructField("transactionId", IntegerType, false), // not nullable
      StructField("itemId", StringType, true),
      StructField("amount", IntegerType, true),
      StructField("date", StringType, true)
    ))

    // Specify the file path
    val filePath = "/path/to/your/parquet/file"

    // Read the Parquet file with the specified schema
    val parquetDf: DataFrame =
      spark.read.schema(fileSchema).format("parquet").load(filePath)

    // Display the DataFrame
    parquetDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
