  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test25 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      //Example of DataFrame operations classified as narrow transformation

      // Sample data
      val data1 = List(1, 2, 3, 4, 5)
      val data2 = List(4, 5, 6, 7, 8)

      // Create RDDs from the sample data
      val rdd1 = spark.sparkContext.parallelize(data1)
      val rdd2 = spark.sparkContext.parallelize(data2)

      // These methods are classified as "narrow transformation"

      // Use 'map' to square each element in rdd1
      val squaredRDD = rdd1.map(x => x * x)

      // Use 'filter' to keep only even numbers in rdd2
      val evenNumbersRDD = rdd2.filter(x => x % 2 == 0)

      // Use 'union' to combine squaredRDD and evenNumbersRDD
      val combinedRDD = squaredRDD.union(evenNumbersRDD)

      // Show the results
      println("Squared RDD:")
      squaredRDD.collect().foreach(println)

      println("\nEven Numbers RDD:")
      evenNumbersRDD.collect().foreach(println)

      println("\nCombined RDD:")
      combinedRDD.collect().foreach(println)

      // Stop the SparkSession
      spark.stop()

    }
  }
