package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test3 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations

    // Create a sample dataset of numbers
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // Parallelize the dataset into an RDD
    val numbersRDD = spark.sparkContext.parallelize(numbers)

    // Filter the even numbers
    // Returns a RDD
    val evenNumbersRDD = numbersRDD.filter(num => num % 2 == 0)

    // Take the first 3 even numbers
    // take returns an array
    // collect() does not take any argument
    // take(Int) returns an array, limit(Int) returns a new DataFrame
    val result = evenNumbersRDD.take(3)

    // Print the result
    println("Filtered and taken numbers:")
    result.foreach(println)

    // Stop the SparkSession
    spark.stop()
  }

}
