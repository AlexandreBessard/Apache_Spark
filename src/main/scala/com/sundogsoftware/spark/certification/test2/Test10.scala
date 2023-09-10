package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test10 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    In a Sort-Merge Join, "skew" refers to an uneven distribution of data between two
    tables or datasets that are being joined. Skew can occur when one of the tables has a significant
    imbalance in the distribution of values within the join key. This imbalance can lead to performance issues,
    as some partitions or tasks may process much more data than others, causing a bottleneck in the join operation.
     */
    // Sample data with skew
    val data1 = Seq(("A", 20), ("B", 30), ("C", 80), ("D", 40), ("E", 50))
    val data2 = Seq(("A", "X"), ("B", "Y"), ("C", "Z"))

    // Create DataFrames
    val df1 = spark.createDataFrame(data1).toDF("key", "value")
    val df2 = spark.createDataFrame(data2).toDF("key", "info")

    // Repartition df1 to handle skew
    val repartitionedDf1 = df1.repartition(5, col("key"))

    // Perform a join between the repartitioned DataFrame and df2
    // We perform an inner join between the repartitioned DataFrame
    // repartitionedDf1 and df2 using the common join key "key."
    val resultDf = repartitionedDf1.join(df2, Seq("key"), "inner")

    // Show the result
    resultDf.show()

    // Stop the SparkSession
    spark.stop()

  }
}
