package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, explode}

object Test19 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data for transactions: (transactionId, storeId, transactionDate, amount)
    val transactions = List(
      (1, "storeA", "2023-10-15", 100.0),
      (2, "storeB", "2023-10-15", 150.0),
      (3, "storeA", "2023-10-15", 110.0),
      (4, "storeC", "2023-10-16", 120.0)
      // ... add more data as needed
    )

    import spark.implicits._
    val transactionsDf = transactions.toDF("transactionId", "storeId", "transactionDate", "amount")

    // Display the initial number of partitions
    println(s"Initial number of partitions: ${transactionsDf.rdd.getNumPartitions}")

    // Use repartition to partition by "storeId" and "transactionDate"
    val repartitionedDf = transactionsDf.repartition(14, $"storeId", $"transactionDate")

    // Display the number of partitions after repartitioning
    println(s"Number of partitions after repartitioning: ${repartitionedDf.rdd.getNumPartitions}")

    // Show the data within each partition
    repartitionedDf.rdd.glom().map(_.toList).collect().zipWithIndex.foreach {
      case (data, partitionNumber) =>
        println(s"\nData in partition $partitionNumber:")
        data.foreach(println)
    }


    // Stop Spark session
    spark.stop()
  }
}
