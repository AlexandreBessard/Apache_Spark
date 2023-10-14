package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Test31 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("RegexReplaceExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("apple", "red", 3),
      ("banana", "yellow", 1),
      ("apple", "green", 5),
      ("apple", "red", 2),
      ("banana", "yellow", 1),
      ("cherry", "red", 3)
    )

    // Create DataFrame
    import spark.implicits._
    val df = data.toDF("fruit", "color", "quantity")

    // Cache the DataFrame
    df.cache()

    // Perform some transformations and actions on the cached DataFrame
    // Since DataFrame is cached, transformations can reuse the cached data, making these operations faster

    // Count the total rows in DataFrame
    println(s"Total rows: ${df.count()}")

    // Show rows where quantity is more than 2
    println("Rows where quantity is more than 2:")
    df.filter($"quantity" > 2).show()

    // Group by fruit name and calculate average quantity
    println("Average quantity per fruit:")
    df.groupBy($"fruit").avg("quantity").show()

    // Group by color and calculate total quantity
    println("Total quantity per color:")
    df.groupBy($"color").sum("quantity").show()

    // Unpersist the DataFrame manually if it's no longer needed
    df.unpersist()

    // Stop the SparkSession
    spark.stop()
  }
}
