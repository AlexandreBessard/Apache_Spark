package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test29 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample data for DataFrame "a"
    val dataA: Seq[(Int, String)] = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Carol")
    )
    val a: DataFrame = dataA.toDF("KeyA", "ValueA")

    // Sample data for DataFrame "b"
    val dataB: Seq[(Int, String)] = Seq(
      (1, "Apple"),
      (2, "Banana"),
      (4, "Cherry")
    )
    val b: DataFrame = dataB.toDF("KeyB", "ValueB")

    // Aliasing DataFrames as "a" and "b" and specifying key columns
    val joinedDF: DataFrame = a.alias("a").join(b.alias("b"), $"a.KeyA" === $"b.KeyB", "inner")

    // Show the resulting joined DataFrame
    joinedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
