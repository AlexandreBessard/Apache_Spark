package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean}

object Test5 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Create a DataFrame
    val data = Seq(
      ("Store1", 1000),
      ("Store2", 1200),
      ("Store3", 800),
      ("Store4", 950),
      ("Store5", 1100)
    )

    val storesDF = spark.createDataFrame(data).toDF("store", "sqft")

    // Describing sqft column
    storesDF.describe("sqft").show()

    // Stop Spark session
    spark.stop()
  }
}
