package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Test16 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for demonstration
    val data = Seq(1, 2, 3, 4, 5)

    // Create a Dataset from the sample data
    import spark.implicits._
    val dataset: Dataset[Int] = data.toDS()

    // Use toLocalIterator to collect data to the driver
    val localIterator = dataset.toLocalIterator

    // Iterate and print the elements in the local iterator
    while (localIterator.hasNext) {
      val element = localIterator.next()
      println(element)
    }

    // Stop the SparkSession
    spark.stop()

  }
}
