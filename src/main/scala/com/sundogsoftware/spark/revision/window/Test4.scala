package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{mean, col}

object Test4 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Create a DataFrame
    val data = Seq(
      ("A", 1000),
      ("B", 1200),
      ("C", 800),
      ("A", 600),
      ("B", 1300)
    )

    val df = spark.createDataFrame(data).toDF("type", "sqft")

    // Calculate mean of sqft column
    df.agg(mean(col("sqft"))).show()

    // Stop Spark session
    spark.stop()
  }
}
