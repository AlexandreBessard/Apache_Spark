package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test15 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder()
        .appName("YearDatasetExample")
        .master("local[*]") // Change to your cluster URL in a real cluster
        .getOrCreate()

    //TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    )

    // Create a DataFrame from the sample data
    val df = spark.createDataFrame(data).toDF("col1", "col2", "col3")

    // Group by an empty list to aggregate the entire DataFrame
    val aggregatedDf = df.groupBy().sum()
    aggregatedDf.show()

    // Collect the result as an array and access the sum
    // collect() returns an array
    // (0) represents the first row and second (0) represents the first column
    val sum = aggregatedDf.collect()(0)(1)

    // Show the result
    println(s"Sum of all columns: $sum")


    // Stop the SparkContext
    spark.stop()
  }
}
