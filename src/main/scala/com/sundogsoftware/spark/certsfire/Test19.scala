package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test19 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, 3),
      (2, 5),
      (3, 6),
      (4, 6),
      (5, 8)
    )

    val transactionsDf = transactionsData.toDF("id", "predError")

    // Filter the DataFrame and count
    val count = transactionsDf.filter(col("predError").isin(3, 6)).count()

    println(s"Number of rows with 'predError' in [3, 6]: $count")

    // Closing the SparkSession
    spark.close()
  }
}
