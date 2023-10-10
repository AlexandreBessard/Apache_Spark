package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


object Test28 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()


    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, 2.0),
      (2, 3.0),
      (3, 4.0),
      (4, null.asInstanceOf[Double]), // value is 0.0
      (5, 5.0)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "value")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Register a UDF
    val pow5UDF = udf((x: Double) => if (x != null) Math.pow(x, 5) else null.asInstanceOf[Double])
    spark.udf.register("power_5_udf", pow5UDF)

    spark.udf.register("power_6_udf", (x: Double) => if (x != null) Math.pow(x, 5) else null.asInstanceOf[Double])

    // Create a temporary view for the DataFrame
    transactionsDf.createOrReplaceTempView("transactions")

    // Execute a SQL query using the UDF
    val resultDf = spark.sql("SELECT power_5_udf(value) AS result FROM transactions")

    // Show the result DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
