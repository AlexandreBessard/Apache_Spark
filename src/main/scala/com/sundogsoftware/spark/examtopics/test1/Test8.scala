package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test8 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data
    val storesDF: DataFrame = Seq(
      ("Store A", "PHYSICAL"),
      ("Store B", "ONLINE"),
      ("Store C", "PHYSICAL"),
      ("Store D", "PHYSICAL")
    ).toDF("StoreName", "modality")

    // Filter the DataFrame based on the "modality" column
    val filteredDF = storesDF.filter(col("modality") === lit("PHYSICAL"))

    // Show the resulting DataFrame
    filteredDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
