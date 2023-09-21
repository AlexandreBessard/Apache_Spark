package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test19 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample store data
    val storesDF: DataFrame = Seq(
      ("Store A", "Division 2", 25000),
      ("Store B", "Division 1", 30000),
      ("Store C", "Division 3", 20000),
      ("Store D", "Division 1", 35000),
      ("Store E", "Division 2", 28000)
    ).toDF("StoreName", "Division", "Sqft")

    // Sort the DataFrame by the "Division" column in ascending order
    /*
    If you add asc() order on "Division"column,
    since it is a string, it will throw an exception
     */
    val sortedDF1 = storesDF.orderBy(col("Sqft").asc)
    val sortedDF = storesDF.orderBy(col("Division"))

    // Show the resulting sorted DataFrame
    sortedDF.show()
    sortedDF1.show()

    // Stop the SparkSession
    spark.stop()
  }

}
