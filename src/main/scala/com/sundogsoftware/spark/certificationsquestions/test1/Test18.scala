package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object Test18 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Reduce DataFrame from 12 to 6 partitions and perform a full shuffle.
     */

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Carol", 22),
      ("David", 28),
      ("Eve", 35),
      ("Frank", 40)
    )

    // Create a DataFrame
    val df = spark.createDataFrame(data).toDF("Name", "Age")

    // Display the original number of partitions
    println(s"Original number of partitions: ${df.rdd.getNumPartitions}")

    // Repartition the DataFrame into 6 partitions
    /*
    repartition() always triggers a full shuffle (different from coalesce())
     */
    val repartitionedDf = df.repartition(5)

    // Display the new number of partitions
    println(s"New number of partitions: ${repartitionedDf.rdd.getNumPartitions}")

    // Stop the SparkSession
    spark.stop()
  }

}
