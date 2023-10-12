package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col, explode, expr, split}

object Test3 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()


    // Example data
    val data = Seq(
      ("div1", "store1"),
      ("div1", "store2"),
      ("div2", "store3"),
      ("div2", "store3"), // duplicate
      ("div3", "store4"),
      ("div3", "store5"),
      ("div3", "store6")
    )

    // Define the DataFrame schema
    val schema = List("division", "store")

    // Create a DataFrame
    val storesDF = spark.createDataFrame(data.map(t => (t._1, t._2))).toDF(schema: _*)

    // Use approx_count_distinct() to estimate the number of distinct divisions
    val distinctCountDF = storesDF.agg(
      approx_count_distinct(col("division")).alias("divisionDistinct"))

    val distinctCountDF1 = storesDF.agg(
      approx_count_distinct(col("division")))

    // Show the result
    distinctCountDF.show()
    distinctCountDF1.show()

    /*
    This code will show an approximate distinct count of the "division" column.
    It’s essential to note that approx_count_distinct() might not always provide the exact count,
    especially in large datasets, but it’s optimized for performance and is particularly useful
     when working with big data, where computational resources might be a concern
     */


    // Stop Spark session
    spark.stop()
  }
}
