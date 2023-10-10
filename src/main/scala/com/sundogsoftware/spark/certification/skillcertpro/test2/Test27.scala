package com.sundogsoftware.spark.certification.skillcertpro.test27


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test27 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("ReduceAndFoldExample")
      .master("local[*]")
      .getOrCreate()

    // Create an RDD of integers
    val numbersRdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))

    // Define a lambda function to sum two integers
    val sum: (Int, Int) => Int = (x, y) => x + y

    /*
    Using reduce(): reduce(sum) is used on numbersRdd to aggregate its elements using the sum
    function, resulting in a single value
     */

    // Use reduce() to aggregate the RDD
    val resultReduce = numbersRdd.reduce(sum) // 1 + 2 + 3 + 4 + 5
    println(s"Sum using reduce: $resultReduce")

    /*
    Using fold(): fold(zeroValue)(sum) is used on numbersRdd to
    aggregate its elements using the sum function,
     but also takes a zero value as an initial value for aggregation
     */

    // Use fold() to aggregate the RDD
    val zeroValue = 0
    // Takes initial value as parameter
    val resultFold = numbersRdd.fold(zeroValue)(sum)
    println(s"Sum using fold: $resultFold")



    // Stop the Spark session
    spark.stop()
  }
}
