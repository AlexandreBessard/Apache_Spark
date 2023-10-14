package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}

object Test12 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // Importing spark implicits
    import spark.implicits._

    // Sample data
    val data = Seq(
      ("A123"),
      ("B456"),
      ("C789"),
      ("D101")
    )

    // Create DataFrame
    val df = data.toDF("id")  // Fixing this line

    // Show original DataFrame
    df.show()

    // Define a regular expression pattern with a capturing group
    // Here, "(\\d+)" is a pattern that matches one or more digits
    val pattern = "(\\d+)"

    // Extract and create a new column with the values from the capturing group
    val extractedDF = df.withColumn("extracted",
      regexp_extract(col("id"), pattern, 1)) // index-based 1

    /*
    If you have a regex pattern like "A(\\d+)B(\\d+)C"
    And a string like: "A123B456C"
    Using regexp_extract(col("your_column"), pattern, 1) will extract 123 because
    123 is matched by the first capturing group (\\d+), and you specified 1 as the group index.

    Using regexp_extract(col("your_column"), pattern, 2) would extract 456 because
    it's matched by the second capturing group and 2 is the group index specified.
     */

    // Show the new DataFrame with the extracted values
    extractedDF.show()

    // Stop Spark session
    spark.stop()
  }
}
