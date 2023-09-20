package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test10 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Which of the following code blocks returns a new DataFrame where
    column productCategories only has one word per row,
    resulting in a DataFrame with many more rows than DataFrame storesDF?
     */

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data containing an array column
    val storesDF: DataFrame = Seq(
      ("Store A", Array("Electronics", "Clothing")),
      ("Store B", Array("Books", "Toys", "Electronics")),
      ("Store C", Array("Clothing"))
    ).toDF("StoreName", "productCategories")

    // Explode the "productCategories" array column into multiple rows
    val explodedDF =
      storesDF.withColumn("productCategories", explode(col("productCategories")))

    // Show the resulting DataFrame
    explodedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
