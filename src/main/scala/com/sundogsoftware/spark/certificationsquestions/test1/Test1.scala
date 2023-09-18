  package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, desc_nulls_first}


  object Test1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()


    // Sample DataFrames
    val transactionsData = Seq(
      (1, "A", 100),
      (2, "B", 200),
      (3, "C", 300)
    )

    val itemsData = Seq(
      (1, "Item1"),
      (2, "Item2"),
      (3, "Item3")
    )

    // Define column names
    val transactionsColumns = Seq("transactionId", "itemId", "amount")
    val itemsColumns = Seq("itemId", "itemName")

    // Create DataFrames
    val transactionsDf = spark.createDataFrame(transactionsData).toDF(transactionsColumns: _*)
    val itemsDf = spark.createDataFrame(itemsData).toDF(itemsColumns: _*)

    // Perform an inner join between transactionsDf and broadcasted itemsDf on the "itemId" column
    /*
    The code performs an inner join, meaning it will include only the rows with matching "itemId"
    values from both DataFrames in the result.
     */
    val resultDf = transactionsDf.join(broadcast(itemsDf), "itemId")

    // Show the result
    resultDf.show() // Show empty because inner join by default shows matching rows

    // Stop the SparkSession
    spark.stop()

  }
}