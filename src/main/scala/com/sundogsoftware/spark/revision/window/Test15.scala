package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test15 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data: (name, score)
    val data = List(
      ("Alice", 50),
      ("Bob", 30),
      ("Charlie", 40),
      ("Alice", 60),
      ("Bob", 70)
    )

    // Create an RDD from the sample data using the SparkContext from SparkSession
    val rdd = spark.sparkContext.parallelize(data)

    // Use reduceByKey to sum scores for each player
    val totalScores = rdd.reduceByKey((a, b) => a + b)

    // Collect and print the results
    totalScores.collect().foreach(println)

    // Stop Spark session
    spark.stop()
  }
}
