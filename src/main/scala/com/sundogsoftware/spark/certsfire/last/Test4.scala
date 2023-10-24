package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // Import Spark's built-in SQL functions

object Test4 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Path to the CSV file
    val filePath = "path/to/your/csvfile.csv"

    // Read the CSV file, filtering out lines that start with a '#'
    val df = spark.read.option("comment", "#").csv(filePath)
    // OR
    val df1 = spark.read.option("comment", "#").format("csv").load(filePath)

    // Get the number of columns
    val numColumns = df.columns.length

    // Print the number of columns
    println(s"The number of columns in the CSV file is: $numColumns")

    spark.stop()
  }
}
