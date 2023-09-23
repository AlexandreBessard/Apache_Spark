package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.CurrentRow.nullable
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructField, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test24 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()


    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample data
    val data = Seq(
      ("John", 25),
      ("Alice", 30),
      ("Bob", 22),
      ("David", 35),
      ("Eve", 28)
    )

    // Create a DataFrame using createDataFrame
    val df: DataFrame = spark.createDataFrame(data).toDF("Name", "Age")

    // Calculate the number of years by adding a constant value (e.g., 5 years) to the "Age" column
    val dfWithYears = df.withColumn("Years", $"Age" + 5)

    // Show the resulting DataFrame
    dfWithYears.show()

    // Stop the SparkSession
    spark.stop()
  }

}
