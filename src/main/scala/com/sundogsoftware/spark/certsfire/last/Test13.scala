package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Test13 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder()
        .appName("YearDatasetExample")
        .master("local[*]") // Change to your cluster URL in a real cluster
        .getOrCreate()

    // TODO: need to be reviewed

    import spark.implicits._

    // Sample dataset of years
    val years = Seq(2020, 2021, 2022, 2023, 2024)

    // Create a Dataset and convert it to a DataFrame
    val yearDataset = spark.createDataset(years)

    // column by default will be named: "value"
    val yearDataFrame = yearDataset.toDF

    // Show the resulting DataFrame
    yearDataFrame.show()

    // Stop the SparkContext
    spark.stop()
  }
}
