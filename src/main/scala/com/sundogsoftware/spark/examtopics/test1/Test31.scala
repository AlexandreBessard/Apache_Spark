package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test31 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample data for DataFrame "df1"
    val data1: Seq[(Int, String)] = Seq(
      (1, "Alice"),
      (2, "Bob")
    )
    val df1: DataFrame = data1.toDF("ID", "Name")

    // Sample data for DataFrame "df2"
    val data2: Seq[(Int, String)] = Seq(
      (3, "Carol"),
      (4, "Dave")
    )
    val df2: DataFrame = data2.toDF("ID", "Name")

    // Use unionByName to combine df1 and df2
    /*
    We use the unionByName method to combine "df1" and "df2."
    The unionByName operation appends rows from "df2" to "df1,"
     and it ensures that the resulting DataFrame has the same schema as "df1."
     */
    val combinedDF: DataFrame = df1.unionByName(df2)

    // Show the resulting combined DataFrame
    combinedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
