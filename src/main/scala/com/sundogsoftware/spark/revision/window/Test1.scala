package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WindowFunctionsExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      ("Customer1", 23000, 40),
      ("Customer2", 45000, 50),
      ("Customer3", 15000, 25),
      ("Customer4", 10000, 20),
      ("Customer5", 30000, 30)
    )

    // Define a schema
    val schema = List(
      "CustomerName",
      "sqft",
      "customerSatisfaction"
    )

    // Creating DataFrame
    val customerDF = spark.createDataFrame(data).toDF(schema: _*)

    // Show original DataFrame
    println("Original DataFrame:")
    customerDF.show()

    // Filtering data with given conditions: sqft <= 25000 or customerSatisfaction >= 30
    val filteredDF = customerDF.filter(
      (col("sqft") <= 25000) or
        (col("customerSatisfaction") >= 30)
    )

    // Show filtered DataFrame
    println("Filtered DataFrame:")
    filteredDF.show()

    // Stop Spark session
    spark.stop()
  }
}
