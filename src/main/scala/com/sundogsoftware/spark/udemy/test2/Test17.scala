package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Test17 {

  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Test17")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    val data = Seq(
      Row(1, 25, 3.0), // Has to be a Row object when using a specific StructType
      Row(2, 30, 2.5),
      Row(3, 25, 1.8),
      Row(4, 35, 4.2),
      Row(5, 25, 3.7)
    )

    // Define the schema for the DataFrame
    val schema = StructType(
      List(
        StructField("transactionId", IntegerType, nullable = false),
        StructField("storeId", IntegerType, nullable = false),
        StructField("predError", DoubleType, nullable = false)
      )
    )

    // Create a DataFrame from the sample data using the schema
    val transactionsDf: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val filteredDf = transactionsDf.filter(col("storeId") === 25)
    // both columns have to be the same to be considered distinct()
    val resultDf = filteredDf.select("predError", "storeId").distinct()

    resultDf.show()

    spark.stop()
  }
}
