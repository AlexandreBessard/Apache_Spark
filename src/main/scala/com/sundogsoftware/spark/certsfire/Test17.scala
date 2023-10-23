package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}  // Import the necessary types

object Test17 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()


    // TODO: need to be reviewed

    val filePath = "/tmp/data.parquet"

    // Reading the parquet file using the predefined schema
    val df = spark.read.schema(
      new StructType(Array(
        StructField("transactionId", IntegerType, nullable = true),
        StructField("predError", IntegerType, nullable = true)
      ))
    ).format("parquet").load(filePath)

    // Displaying the DataFrame
    df.show()

    // Closing the SparkSession
    spark.close()
  }
}
