package com.sundogsoftware.spark.revision

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring_index}

object Test1 {

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
      .withColumn("first_name", substring_index(col("full_name"), "-", 1))
      // -1 means extract the substring from the end of the input string
      .withColumn("last_name", substring_index(col("full_name"), "-", -1))
      .withColumn("test", substring_index(col("full_name"), "-", -2))

    // Show the result
    resultDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
