package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test26 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "error1", 12.5),
      (2, "error2", 15.5),
      (3, "error3", 14.5),
      (4, "error4", 13.0)
    )

    // Creating DataFrame
    val transactionsDf = data.toDF("transactionId", "predError", "value")

    // Show original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Filtering rows where 'transactionId' is even and selecting "predError" and "value" columns
    val filteredDf = transactionsDf
      .filter(col("transactionId") % 2 === 0)
      .select("predError", "value") //select multiple columns

    // Show filtered DataFrame
    println("Filtered DataFrame:")
    filteredDf.show()


    // Stop Spark session
    spark.stop()
  }
}
