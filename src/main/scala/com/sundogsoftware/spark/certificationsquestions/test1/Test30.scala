package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Test30 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("RegexReplaceExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      Row("apple", "red"),
      Row("banana", "yellow"), // duplicate
      Row("apple", "green"),
      Row("apple", "red"),
      Row("banana", "yellow"), // duplicate
      Row("cherry", "red")
    )

    // Define a schema
    val schema = StructType(Array(
      StructField("fruit", StringType, true),
      StructField("color", StringType, true)
    ))

    // Create DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show original DataFrame
    println("Original DataFrame:")
    df.show()

    // Drop duplicate rows
    val distinctDF = df.dropDuplicates()

    // Show DataFrame after dropping duplicates
    println("DataFrame after dropping duplicates:")
    distinctDF.show()

    // Drop duplicates based on selected columns
    val distinctColDF = df.dropDuplicates("fruit")


    // Show DataFrame after dropping duplicates based on "fruit" column
    println("DataFrame after dropping duplicates based on 'fruit' column:")
    distinctColDF.show()

    // Drop duplicate rows based on selected columns
    val selectedDistinctDF1 = df.dropDuplicates("fruit", "color")

    println("DataFrame after dropping duplicates based on 'fruit' and 'color' columns:")
    selectedDistinctDF1.show()

    // Stop the SparkSession
    spark.stop()
  }
}
