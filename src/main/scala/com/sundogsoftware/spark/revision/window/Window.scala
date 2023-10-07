package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WindowFunctionsExample")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      ("A", 1000, "01/2021"),
      ("A", 1500, "02/2022"),
      ("A", 2000, "03/2023"),
      ("B", 2000, "01/2024"),
      ("B", 2500, "02/2025"),
      ("B", 2000, "03/2026")
    )

    val df = spark.createDataFrame(data)
      .toDF("Region", "Sales", "Date")

    // orderBy is in ascending order by default
    val windowSpec = Window.partitionBy("Region").orderBy("Date")
    // orderBy in descending order
    val windowSpec1 = Window.partitionBy("Region").orderBy(col("Date").desc)
    /*
    Returns the rank of rows within a window partition, with the same rank for equal values and leaving gaps.
     */
    val rankedDF = df
      .withColumn("rank", rank().over(windowSpec))

    val cumSumDF = df
      .withColumn("cumulative_sum", sum("Sales").over(windowSpec))

    val lagDF = df
      .withColumn("prev_month_sales", lag("Sales", 1).over(windowSpec))

    val leadDF = df
      .withColumn("next_month_sales", lead("Sales", 1).over(windowSpec))

    val cumeDistDF = df
      .withColumn("cume_dist", cume_dist().over(windowSpec))

    val percentRankDF = df
      .withColumn("perc_rank", percent_rank().over(windowSpec))

    println("Rank() : ")
    rankedDF.show()


    cumSumDF.show()
    lagDF.show()
    leadDF.show()
    cumeDistDF.show()
    percentRankDF.show()

    spark.stop()
  }
}
