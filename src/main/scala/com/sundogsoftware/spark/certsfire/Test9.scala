package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}  // Required for StringType

object Test9 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //TODO: need to be reviewed

    //Append to an existing parquet file.

    // Sample data for the transactionsDf DataFrame
    val transactionsData = Seq(
      (1, "apple", 150.0),
      (2, "banana", 200.0),
      (3, "cherry", 75.0)
    )

    val transactionsDf = transactionsData.toDF("id", "item", "price")

    // Display the DataFrame (optional)
    transactionsDf.show()

    // Define the path where you want to save the parquet file
    // Make sure to modify this path as per your local file system or HDFS
    val path = "/path/to/output_directory"

    // Save the DataFrame to the specified path in parquet format
    transactionsDf.write.format("parquet").option("mode", "append").save(path)

    println(s"Data saved to path: $path")

    // Closing the SparkSession
    spark.close()
  }
}