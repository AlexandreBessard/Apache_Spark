package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()


    // TODO: need to be reviewed

    import spark.implicits._

    // Sample Data for transactionsDf
    val transactionsData = Seq(
      (1, "apple", 101),
      (2, "banana", 25),
      (3, "cherry", 102),
      (4, "date", 25),
      (5, "fig", 103)
    )

    val transactionsDf =
      transactionsData.toDF("transactionId", "productName", "storeId")

    // Filter rows where storeId is not equal to 25
    val filteredDf = transactionsDf.where(transactionsDf("storeId") =!= 25)

    // Display the filtered DataFrame
    filteredDf.show()

    spark.stop()
  }
}
