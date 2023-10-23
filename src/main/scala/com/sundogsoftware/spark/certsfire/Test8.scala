package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types.StringType  // Required for StringType

object Test8 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //TODO: need to be reviewed

    // Create a DataFrame with date strings
    val dfDates = Seq(
      ("23/01/2022 11:28:12"),
      ("24/01/2022 10:58:34")
    ).toDF("date")

    // Convert date strings to timestamp format
    val dfConvertedDates = dfDates.withColumn("timestamp", to_timestamp(col("date"), "dd/MM/yyyy HH:mm:ss"))

    // Show the DataFrame
    dfConvertedDates.show()

    // Closing the SparkSession
    spark.close()
  }
}