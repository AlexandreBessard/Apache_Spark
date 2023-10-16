package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test17 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()


    // Define the file path to your Parquet file
    val filePath = "path_to_parquet_file.parquet" // Replace with your actual file path
    /*
    "mergeSchema": This option is about handling the schema of the Parquet files.
    "true": This indicates that you want to enable schema merging.

    When you set mergeSchema to true, Spark will merge differing schemas (different column types and columns)
    found in the Parquet data source into one consolidated schema, and load it appropriately.
    This is useful when you have parquet files with different schemas in the same directory,
    and you want Spark to reconcile and merge them while reading.
     */

    // Read the Parquet file with schema merging option
    val parquetDf: DataFrame = spark.read.option("mergeSchema", "true").parquet(filePath)

    // Show the contents of the DataFrame
    parquetDf.show()

    /*

    Without real data and execution, it's not possible to provide an exact output.
    However, I can illustrate what you might expect to see based on hypothetical data given our example.

    Suppose:

    users1.parquet contains:
    name | age
    -----------------
    Alex | 30
    Bob  | 22

    users2.parquet contains:
    name  | age | city
    -----------------------
    Chris | 33  | London
    Dan   | 29  | New York
    If you run the code reading these files with the "mergeSchema", "true" option,
    the resulting DataFrame (parquetDf) might look something like this when displayed with show():

    plaintext
    Copy code
    +-----+---+--------+
    | name|age|    city|
    +-----+---+--------+
    | Alex| 30|    null|
    |  Bob| 22|    null|
    |Chris| 33|  London|
    |  Dan| 29|New York|
    +-----+---+--------+
    Explanation:

    The rows from users1.parquet and users2.parquet are combined into a single DataFrame.
    The merged schema becomes ["name", "age", "city"] since we opted to merge schemas.
    null values appear in the city column for rows originating from users1.parquet since that file doesn't have a city column.
     */

    // Stop the SparkSession
    spark.stop()
  }
}
