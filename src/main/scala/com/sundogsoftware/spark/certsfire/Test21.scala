package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test21 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample DataFrame
    val transactionsDf = Seq(
      (1, 100.0),
      (2, 200.5),
      (3, 150.0),
      (4, 220.0),
      (5, 100.0)
    ).toDF("productId", "value")

    // Select 'value' and union it with 'productId', then get distinct values
    val resultDf = transactionsDf.select('value)
      .union(transactionsDf.select('productId))
      .distinct()

    // Show the resulting DataFrame
    resultDf.show()

    spark.stop()
  }
}
