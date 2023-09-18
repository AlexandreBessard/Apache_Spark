package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.storage.StorageLevel

object Test4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data with nested arrays
    val data = Seq(
      (1, "A", Array("X", "Y", "Z")),
      (2, "B", Array("P", "Q")),
      (3, "C", Array("R")),
      (4, "D", Array("S", "T", "U"))
    )

    // Define schema
    val schema = Seq(
      "id", "value", "arrayColumn"
    )

    // Create a DataFrame
    val df = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    df.show()

    // Use explode() to unnest the arrayColumn
    /*

    The explode() function in Apache Spark is used to transform an array
    or map column into multiple rows, effectively "exploding" the array or
    map elements into separate rows.
     */
    val explodedDf = df.withColumn("explodedValue", explode(col("arrayColumn")))

    // Display the DataFrame after exploding the arrayColumn
    println("DataFrame after using explode():")
    explodedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
