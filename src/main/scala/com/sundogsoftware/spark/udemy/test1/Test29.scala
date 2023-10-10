package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring_index}

object Test29 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample DataFrame with a column containing strings
    val data = Seq(("John-Doe-Engineer"), ("Alice-Smith-Designer"), ("Bob-Jones-Manager"))

    val df = data.toDF("full_name")

    df.printSchema()

    // Split the "full_name" column into "first_name" and "last_name" using "-" as the delimiter
    //Start with index based 1
    val resultDF = df
      // 1 means first element from the beginning
      .withColumn("first_name", substring_index(col("full_name"), "-", 1))
      // -1 means extract the substring from the end of the input string
      // first element from the end of the string
      .withColumn("last_name", substring_index(df("full_name"), "-", -1))
      // start from the end and get the elements 1 and 2 from the end.
      .withColumn("test", substring_index(col("full_name"), "-", -2))
      // start from the start and get the element 1 and 2 from the start. 2 is included in the result
      .withColumn("first_name1", substring_index(col("full_name"), "-", 2))

    // Show the result
    resultDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
