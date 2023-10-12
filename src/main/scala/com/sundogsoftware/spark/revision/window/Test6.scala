package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test6 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // Create a DataFrame
    val data = Seq(
      ("Store1", 1000),
      ("Store2", 1200),
      ("Store3", 800),
      ("Store4", 950),
      ("Store5", 1100)
    )

    val storesDF = spark.createDataFrame(data).toDF("store", "sqft")

    //Retrieve the first row and Get 'sqft' value from the first row
    val sqftValue = storesDF.first().getAs[Int]("sqft")

    // Display sqft value
    println(s"The 'sqft' value from the first row is: $sqftValue")

    // Stop Spark session
    spark.stop()
  }
}
