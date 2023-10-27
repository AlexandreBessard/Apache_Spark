package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // Import Spark's built-in SQL functions

object Test3 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, 0.5),
      (2, 1.0),
      (3, 1.5),
      (4, 2.0)
    )

    val transactionsDf = transactionsData.toDF("transactionId", "value")

    // Add a column with the cosine of the value column converted to degrees and rounded to two decimals
    val resultDf =
      transactionsDf
        .withColumn("cos", round(cos(toDegrees($"value")), 2))

    // Display the resulting DataFrame
    resultDf.show()

    spark.stop()
  }
}
