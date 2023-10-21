package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test29 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      (1, "Transaction1", 100.5),
      (2, "Transaction2", 200.8),
      (3, "Transaction3", 50.2)
    )

    // Creating DataFrame from the sample data
    val transactionsDf = data
      .toDF("transactionId", "transactionName", "amount")

    // Directory to store the Parquet files
    val storeDir = "path/to/your/output/directory"

    // Write DataFrame to Parquet format with Snappy compression
    transactionsDf.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save(storeDir)

    println(s"Data written to $storeDir in Parquet format with Snappy compression.")

    // Stop Spark session
    spark.stop()
  }
}
