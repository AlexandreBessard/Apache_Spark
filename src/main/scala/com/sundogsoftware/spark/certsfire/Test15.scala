package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, lower, regexp_replace}

object Test15 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      ("ABC-123-XYZ"),
      ("DEF-456-PQR"),
      ("GHI-789-STU")
    )

    val transactionsDf = data.toDF("itemName")

    // Processing the itemName column
    val processedDf = transactionsDf
      .withColumn("processedItem",
        lower(regexp_replace(col("itemName"), "-", ""))) // convert to lowercase and remove hyphens

      .withColumn("processedItemLength",
        functions.length(col("processedItem")).alias("length")) // calculate the length and alias it

    // Display the result DataFrame
    processedDf.show()

    // Closing the SparkSession
    spark.close()
  }
}
