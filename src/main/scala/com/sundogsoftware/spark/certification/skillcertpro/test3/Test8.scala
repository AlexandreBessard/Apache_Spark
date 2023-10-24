  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test8 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Define a UDF to calculate the square of a Long value
    val squared = (s: Long) => { s * s }
    spark.udf.register("square", squared)

    // Create a DataFrame with a range of values from 1 to 19
    spark.range(1, 20).createOrReplaceTempView("test")

    // Use Spark SQL to select the "id" column and apply the "square" UDF to calculate the "id_squared" column
    val result = spark.sql("SELECT id, square(id) as id_squared FROM test")

    // Show the result DataFrame
    result.show()

    // Stop the SparkSession
    spark.stop()

  }
}