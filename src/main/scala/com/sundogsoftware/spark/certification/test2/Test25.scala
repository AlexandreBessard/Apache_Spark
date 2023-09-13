  package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc_nulls_first


  object Test25 {

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
      (2, null),
      (3, "Bob"),
      (4, null),
      (5, "Charlie")
    )

    // Define the schema
    val schema = List("ID", "a")

    // Create a DataFrame
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Order the DataFrame by column "a" in descending order with nulls first
    val orderedDF = df.orderBy(desc_nulls_first("a"))

    // Show the ordered DataFrame
    println("Ordered DataFrame:")
    orderedDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}