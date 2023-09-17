  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


  object Test3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    )

    // Define the schema
    val schema = List("a", "Name")

    // Create a DataFrame
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Add a new column "casted" with string values from column "a"
    val resultDF = df.withColumn("casted", col("a").cast("string"))

    // Show the result DataFrame
    println("Result DataFrame:")
    resultDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}