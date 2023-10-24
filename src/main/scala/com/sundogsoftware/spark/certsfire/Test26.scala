package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, to_timestamp}  // Corrected this import

object Test26 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    /*
    1.
    root
    2.
    |-- transactionId: integer (nullable = true)
    3.
    |-- predError: integer (nullable = true)
    4.
    |-- value: integer (nullable = true)
    5.
    |-- storeId: integer (nullable = true)
    6.
    |-- productId: integer (nullable = true)
    7.
    |-- f: integer (nullable = true)
    Schema of second partition:
    1.
    root
    2.
    |-- transactionId: integer (nullable = true)
    3.
    |-- predError: integer (nullable = true)
    4.
    |-- value: integer (nullable = true)
    5.
    |-- storeId: integer (nullable = true)
    6.
    |-- rollId: integer (nullable = true)
    7.
    |-- f: integer (nullable = true)
    8.
    |-- tax_id: integer (nullable = false)
     */

    /*
    Given that you have two partitions with slightly different schemas, you'll want to ensure
    that when reading the parquet file you'll get a unified DataFrame with all columns present.

    In Apache Spark, the read API will automatically handle this for Parquet files. Specifically,
    when reading a parquet file, Spark provides a schema merging feature which merges the different schemas
    from different partitions into a single DataFrame with all columns.
     */

    val filePath: String = "path/to/your/parquet"

    val df = spark.read.option("mergeSchema", "true").parquet(filePath)
    df.printSchema()

    spark.stop()
  }
}
