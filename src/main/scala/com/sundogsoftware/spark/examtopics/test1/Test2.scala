package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test2 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Create a new DataFrame from an existing one with more partition.
     */


    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data
    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 28),
      ("David", 22),
      ("Eve", 35)
    ).toDF("Name", "Age")

    // Repartition the DataFrame into 12 partitions
    val repartitionedData = data.repartition(12)

    // Check the number of partitions
    val numPartitions = repartitionedData.rdd.partitions.length

    // Show the number of partitions
    println(s"Number of partitions: $numPartitions")

    // Stop the SparkSession
    spark.stop()
  }

}
