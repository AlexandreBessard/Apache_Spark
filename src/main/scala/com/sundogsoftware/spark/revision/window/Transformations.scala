package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Transformations {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WindowFunctionsExample")
      .master("local[*]")
      .getOrCreate()

    /*
    Transformation functions, most used one
     */

    import org.apache.spark.sql.functions._

    // Sample DataFrame with sales data
    val salesData = Seq(
      ("apple", "2023-01-01", 100),
      ("banana", "2023-01-01", 150),
      ("apple", "2023-01-02", 200),
      ("apple", "2023-01-03", 300),
      ("banana", "2023-01-03", 250),
      ("cherry", "2023-01-03", 100)
    )

    // Define the schema for the DataFrame
    val schema = List(
      "product", "date", "amount"
    )

    // Create a DataFrame from the sample data and schema
    val salesDF = spark.createDataFrame(salesData).toDF(schema: _*)

    // 1. filter(): Filter out the data for 'apple'
    val appleSalesDF = salesDF.filter(col("product") === "apple")

    // 2. select(): Select only the 'date' and 'amount' columns
    val dateAndAmountDF = salesDF.select("date", "amount")

    // 3. groupBy() and agg(): Get the total sales amount per product
    val totalSalesPerProductDF = salesDF
      .groupBy("product")
      .agg(
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount"),
        min("amount").alias("min_amount")
      )

    println("Original Sales Data:")
    salesDF.show()

    println("Sales Data for Apple:")
    appleSalesDF.show()

    println("Date and Amount Data:")
    dateAndAmountDF.show()

    println("Total Sales per Product:")
    totalSalesPerProductDF.show()

    // TODO: need to be reviewed
    /*
    Wide transformation involves data shuffling:
    groupByKey(), reduceBy(), repartition(), distinct(), orderBy()
     */

    /*
    Narrow transformation does NOT involve data shuffling:
    map(), filter(), filter(), union()
     */

    // Stop Spark session
    spark.stop()
  }
}
