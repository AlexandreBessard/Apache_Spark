package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object Test6 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data as a Seq of rows
    /*
    So, the result of this transformation is a new sequence of Row objects,
    where each Row represents one of the dates from the original sequence.
    This is a common technique when you want to create a sequence of Row objects
    from some other data structure so that you can then use it to create a DataFrame
    in Spark.
     */
    val data = Seq(("2023-09-09"), ("2023-08-15"), ("2023-07-01")).map(date => Row(date))

    // Define the schema for the DataFrame
    val schema = List("today")

    // Create an RDD of Row objects
    val rdd = spark.sparkContext.parallelize(data)

    // Create a DataFrame with a "today" column
    val df: DataFrame = spark.createDataFrame(rdd, org.apache.spark.sql.types.StructType(
      schema.map(fieldName => org.apache.spark.sql.types.StructField(fieldName, org.apache.spark.sql.types.StringType, nullable = true))
    ))

    // Add a new column "week_ago" by subtracting 7 days from the "today" column
    val resultDf = df.withColumn("week_ago", date_sub(col("today"), 7))

    // Show the result
    resultDf.show()

    // Stop the SparkSession
    spark.stop()

  }
}
