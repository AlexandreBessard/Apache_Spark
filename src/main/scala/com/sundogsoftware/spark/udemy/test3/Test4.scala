package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test4 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test3")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data (Replace with your actual data or file path)
    val data = Seq(
      (1, "Thick Coat for Walking in the Snow",
        Array("blue", "winter", "cozy"), "Sports Company Inc."),

      (2, "Elegant Outdoors Summer Dress",
        Array("red", "summer", "fresh", "cooling"), "YetiX"),

      (3, "Outdoors Backpack",
        Array("green", "summer", "travel"), "Sports Company Inc.")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "attributes", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Create an RDD from the DataFrame
    val itemsRdd = itemsDf.rdd

    // Create a LongAccumulator to count the rows where 'Inc.' is in 'supplier'
    val accum = spark.sparkContext.longAccumulator("accumulatorName")

    // Function to check if 'Inc.' is in 'supplier' and increment the accumulator
    def checkIfIncInSupplier(row: org.apache.spark.sql.Row): Unit = {
      val supplier = row.getAs[String]("supplier")
      val supplier1 = row.getAs[String](3) // index-based 0 column index.
      if (supplier.contains("Inc.")) {
        println(supplier1)
        accum.add(1)
      }
    }
    // Apply the checkIfIncInSupplier function to each row in the RDD
    itemsRdd.foreach(checkIfIncInSupplier)

    // Print the value of the accumulator
    println("Result : -> " + accum.value)
    // Stop the SparkSession
    spark.stop()
  }
}
