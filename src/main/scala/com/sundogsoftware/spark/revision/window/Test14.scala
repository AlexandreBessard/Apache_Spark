package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean} // import mean from Spark functions

object Test14 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val storesDF = Seq(
      (1, "StoreA", 1000),
      (2, "StoreB", 1500),
      (3, "StoreC", 1200),
      (4, "StoreD", 1100)
    ).toDF("storeId", "storeName", "sqft")

    storesDF.show()

    // Aggregate to compute the mean of sqft column
    val sqftMeanDF = storesDF.agg(mean(col("sqft")).alias("sqftMean"))
    sqftMeanDF.show()

    // Stop Spark session
    spark.stop()
  }
}
