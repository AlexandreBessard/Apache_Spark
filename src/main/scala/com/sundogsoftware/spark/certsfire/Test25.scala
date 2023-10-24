package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, max, min, to_timestamp}  // Corrected this import

object Test25 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data with transactionDate
    val data = Seq(
      ("2023-01-01"), // Is in string format
      ("2023-01-02"),
      ("2023-01-03"),
      ("2023-01-04")
    )

    val transactionsDf = data.toDF("transactionDate")

    // Transform the date into the desired format and store in a new column
    val transformedDf = transactionsDf.withColumn(
      "transactionDateForm",
      date_format($"transactionDate", "MMM d (EEEE)"))

    val timestampedDf1 = transactionsDf.withColumn(
      "transactionTimestamp",
      to_timestamp($"transactionDate")
    )

    transactionsDf.printSchema()
    timestampedDf1.printSchema()

    // Display the result
    transformedDf.show()
    timestampedDf1.show()

    spark.stop()
  }
}
