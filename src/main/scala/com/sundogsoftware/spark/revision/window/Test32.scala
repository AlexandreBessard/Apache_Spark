package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Test32 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Setting up Spark configuration and specifying the spark.default.parallelism property
    val conf = new SparkConf()
      .setAppName("ParallelismExample")
      .setMaster("local[*]")
      /*
      In Spark, the parallelism level determines how many tasks can run concurrently.
      This in turn affects how well your job can utilize the resources available to it.
       If the parallelism level is set too low, there might not be enough tasks to
       fully utilize the resources. If set too high, there might be too much
       overhead from managing many small tasks.

       For distributed shuffle operations like reduceByKey and groupByKey,
       the default number of partitions is set by the spark.default.parallelism configuration.
      For operations like map and filter that do not involve a shuffle,
      the number of tasks is typically set to the number of input partitions.
       */
      .set("spark.default.parallelism", "30")

    // Creating a SparkContext
    val sc = new SparkContext(conf)

    // Creating an RDD from a range of numbers. Thanks to spark.default.parallelism, this RDD will have 30 partitions
    val rdd = sc.parallelize(1 to 10000)

    // Applying a transformation to each number in the RDD
    val transformedRDD = rdd.map(num => num * 2)

    // Performing an action to collect the results and printing the first 10 numbers from the transformed RDD
    transformedRDD.take(10).foreach(println)

    // Stop the SparkContext
    sc.stop()
  }
}
