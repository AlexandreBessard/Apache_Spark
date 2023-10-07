package com.sundogsoftware.spark.certification.skillcertpro.test2

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

    // Create a DataSet and a temporary view
    /*
    In other words, it's like a DataFrame, but provides more type safety and object-oriented programming style.
    Introduced in Spark 1.6, the DataSet API brings together the best of both worlds: the expressive power and
    benefits of RDDs and the optimizations provided by the Spark SQL logical query plan.
     */
    // Generate from number 1 to 19 (20 is excluded)
    // The column name by default in our case will be "id"
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
