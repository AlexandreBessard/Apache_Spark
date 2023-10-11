package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split, explode, expr}

object Test2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    import spark.implicits._

    // Example 1: Using split()
    val data1 = Seq("apple-10", "banana-20", "cherry-30")
    val df = spark.createDataFrame(data1.map(Tuple1.apply)).toDF("info")

    val splitDF = df.withColumn("info_array", split(col("info"), "-"))

    println("DataFrame after using split():")
    splitDF.show()

    // Example 2: Using explode() and expr()
    val data2 = Seq(
      (1, "apple,orange,banana"),
      (2, "grape,apple,mango,cherry,1"),
      (3, "blueberry,blackberry,3")
    )

    val fruitsDF = data2.toDF("id", "fruits")

    val explodedDF = fruitsDF.select($"id", explode(expr("split(fruits, ',')")).as("fruit"))

    println("DataFrame after using explode():")
    explodedDF.show()

    // Assuming each fruit string may contain a digit at the end, and we want to extract it.
    val extractedDF = explodedDF.select(
      $"id",
      $"fruit",
      expr("regexp_extract(fruit, '\\\\d+$', 0)").as("code")
    )

    println("DataFrame after using expr() for extraction:")
    extractedDF.show()

    // Stop Spark session
    spark.stop()
  }
}
