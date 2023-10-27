package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Test12 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkConf and SparkContext
    val conf = new SparkConf().setAppName("RDDExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: need to be reviewed

    // Sample data: (productCategory, salesAmount)
    val data = List(
      ("Electronics", 500.0),
      ("Clothing", 300.0),
      ("Electronics", 700.0),
      ("Books", 200.0),
      ("Clothing", 400.0))

    // Create an RDD from the data
    val rdd = sc.parallelize(data)

    // Equivalent using groupBy() on DataFrame instead of RDDs
    // Use RDD transformations and actions to precisely instruct Spark
    val categorySales = rdd
      .map { case (category, amount) => (category, amount) } // Map the data to (category, amount)
      .reduceByKey(_ + _) // Reduce by key to sum sales amount per category

    // Show the result
    categorySales.collect().foreach(println)

    // Stop the SparkContext
    sc.stop()
  }
}
