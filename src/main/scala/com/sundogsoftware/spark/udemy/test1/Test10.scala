package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


object Test10 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Compare the syntax between these sort methods, both output the same result
     */

    // Sample data
    val data = Seq(("A", 10), ("B", null), ("C", 5), ("D", null), ("E", 8))

    // Define schema
    val schema = StructType(
      StructField("Transaction", StringType, nullable = true) ::
        StructField("predError", IntegerType, nullable = true) :: Nil
    )

    // Create DataFrame with the specified schema
    val rows = data.map { case (transaction, predError) =>
      Row(transaction, predError)
    }
    val transactionsDf = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    // Sorting in descending order
    // All null value are placed at the end
    val sortedDf = transactionsDf.sort(col("predError").desc)
    //OR
    // Sorting with nulls last
    val sortedDfNullsLast =  // takes string as parameter
    transactionsDf.sort(desc_nulls_last("predError"))

    sortedDf.show()
    sortedDfNullsLast.show()

      // Stop the SparkSession
    spark.stop()
  }
}
