package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{StructField, StructType, StringType}

object Test29 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("RegexReplaceExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("A123XYZ"),
      ("B456XYZ"),
      ("C789XYZ"),
      ("D101XYZ")
    )

    // Define a schema
    val schema = StructType(Array(StructField("alphanumeric", StringType, nullable = true)))

    // Convert Seq[String] to Seq[Row]
    val rowData = data.map(Row(_))

    // Create DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)

    // Show original DataFrame
    df.show()

    // Define a regular expression pattern to match
    // Here, "\\d+" is a pattern that matches one or more digits
    val pattern = "\\d+"

    // Replace the matched pattern with a replacement string
    // In this case, replace all digits with the string "NUMBER"
    val replacedDF = df.withColumn("replaced",
      regexp_replace(col("alphanumeric"), pattern, ""))

    // When replacement is empty : "", remove the string based on the pattern

    // Show the new DataFrame with the replaced values
    replacedDF.show()

    // Stop the SparkSession
    spark.stop()
  }
}
