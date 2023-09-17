  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, udf}
import org.apache.spark.sql.types.IntegerType


  object Test4 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
/*
    val data = Seq(
      (1, "Alice", 50000),
      (2, "Bob", 2),
      (3, "Charlie", 60000),
      (4, "Alex", 55000)
    )
*/

    val data = Seq(
      (1, "Alice", 50000),
      (2, "Bob", 0),
      (3, "Charlie", 60000),
      (4, null, 55000)
    )

    // Define the schema
    val schema = List("ID", "Name", "Bonus")

    // Create a DataFrame
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Define a User-Defined Function (UDF) to calculate salaries
    spark.udf.register("calculateSalary", (name: String, bonus: Integer) => {
      if (name != null && bonus != null) {
        // Calculate salary with a bonus
        bonus + 5000
      } else {
        // Handle null values or conditions as needed
        0 // You can customize the handling of null values
      }
    }, IntegerType)

    // Apply the UDF to calculate salaries and handle null values
    val resultDF = df.withColumn("Salary", expr("calculateSalary(Name, Bonus)"))

    // Show the result DataFrame
    println("Result DataFrame:")
    resultDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}