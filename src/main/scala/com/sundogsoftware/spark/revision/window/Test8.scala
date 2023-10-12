package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, udf}

object Test8 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Create a DataFrame
    val data = Seq(
      ("Store1", "2023-01-01"),
      ("Store2", "2023-02-15"),
      ("Store3", "2023-05-10"),
      ("Store4", "2023-07-23"),
      ("Store5", "2023-10-30")
    )

    val storesDF = spark.createDataFrame(data).toDF("store", "openDate")

    // Add and modify columns
    val modifiedDF = storesDF
      .withColumn("openTimestamp", col("openDate").cast("Timestamp"))
      .withColumn("month", month(col("openTimestamp")))

    // Display the result
    modifiedDF.show()

    // Stop Spark session
    spark.stop()
  }
}
