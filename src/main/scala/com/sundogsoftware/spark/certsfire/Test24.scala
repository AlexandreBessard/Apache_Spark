package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}  // Corrected this import

object Test24 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, 10.5),
      (2, 20.0),
      (1, 15.0),
      (2, 25.5),
      (3, 30.5)
    )

    val transactionsDf = transactionsData.toDF("productId", "value")

    // Grouping by productId and aggregating to get the highest and lowest value for each productId
    val aggregatedDf = transactionsDf.groupBy("productId")
      .agg(max("value").alias("highest"), min("value").alias("lowest"))

    // Showing the result
    aggregatedDf.show()

    spark.stop()
  }
}
