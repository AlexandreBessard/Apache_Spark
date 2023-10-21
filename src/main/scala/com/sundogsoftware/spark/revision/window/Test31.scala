package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf  // Import for UDF
import org.apache.spark.sql.types.LongType  // Import for LongType

object Test31 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data: Let's assume 'transactions' DataFrame has a column named 'value' of LongType
    val transactionsDf =
      spark.createDataFrame(Seq((1L), (2L), (3L), (4L), (5L), (6L))
        .map(Tuple1.apply)).toDF("value")

    // Register the DataFrame as a temporary view
    transactionsDf.createOrReplaceTempView("transactions")

    // Define the UDF
    val pow5 = udf((x: Long) => x * x * x * x * x)

    /*
    In Spark, a UDF (User-Defined Function) is considered deterministic by default.
    This means Spark assumes that given the same input, the UDF will always produce the same output.
    This assumption can lead to optimizations like caching.

    However, there are times when your UDF might not produce the same output for the same input.
    For example, it might generate a random number, look up current time, or fetch results from an external system.
    In such cases, the UDF is non-deterministic.

    asNondeterministic is a method you can call on a UDF to explicitly mark it as non-deterministic.
    This informs Spark that the UDF can return different results even with the same input, so Spark won't apply certain optimizations to it.

    In simple terms:

    A deterministic UDF: Same input → Always the same output.
    A non-deterministic UDF: Same input → Possibly different output on different calls.
    asNondeterministic is used to tell Spark, "Hey, this UDF is of the second type!".
     */

    // Register the UDF
    spark.udf.register("power_5_udf", pow5.asNondeterministic()) // possibility of different output

    // Use the UDF in SQL query
    val result =
      spark.sql("SELECT power_5_udf(value) AS powered_value FROM transactions")
    result.show()

    // Stop Spark session
    spark.stop()
  }
}
