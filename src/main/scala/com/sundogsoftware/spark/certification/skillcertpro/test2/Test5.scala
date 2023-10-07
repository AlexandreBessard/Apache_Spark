package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test5 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val rawData = Seq(
      ("A", "20"),
      ("B", "30"),
      ("C", "80"),
      ("D", "40"),
      ("E", "50"),
      ("F", "60")
    )

    // Create a DataFrame
    val df: DataFrame = spark.createDataFrame(rawData).toDF("Letter", "Number")

    // Specify the percentage of random records to sample (25% in this case)
    val samplePercentage = 0.25

    // Randomly sample 25% of records without replacement
    // false means without
    /*
    withReplacement = false: When you set withReplacement to false,
    it means that you don't allow the same data point to be selected more than once
    in your sample. In other words, each data point in your sample will be unique and won't appear again in the sample. This is similar to randomly drawing marbles from a bag without putting them back.

    withReplacement = true: Conversely, when you set withReplacement to true,
    it means that you allow the same data point to be selected multiple times in your sample.
    In this case, you might see some data points appear more than once in your sample.
    This is similar to randomly drawing marbles from a bag and putting them back after each draw,
     allowing the same marble to be drawn again in the next round.
     */
    val sampledDf = df.sample(withReplacement = false, fraction = samplePercentage)

    /*
    You can use the sample() method in Spark to randomly select a specified percentage of records from a DataFrame without replacement
     */
    // Show the sampled DataFrame
    sampledDf.show()

    // Stop the SparkSession
    spark.stop()

  }
}
