package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test9 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Define the UDF
    val squared = (s: Long) => {
      s * s
    }
    spark.udf.register("square", squared)

    // Create a DataFrame and a temporary view
    val df = spark.range(1, 20)
    /*
    By default, the generated DataFrame has a single column with the name "id."
     */
    df.createOrReplaceTempView("test")
    println("---> ")
    df.show()

    // Use the UDF in a SQL query
    val resultDf = spark.sql("SELECT id, square(id) as id_squared FROM test")

    // Show the result
    resultDf.show()

    // Stop the SparkSession
    spark.stop()

  }
}
