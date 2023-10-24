package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession // Import Spark's built-in SQL functions

object Test8 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    import spark.implicits._

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, 1),
      (2, 2),
      (3, 3)
    )

    val transactionsDf = transactionsData.toDF("transactionId", "storeId")

    // Print the schema to display information about column 'storeId'
    // and transactionId only
    transactionsDf.select("storeId", "transactionId").printSchema()
    
    spark.stop()
  }
}
