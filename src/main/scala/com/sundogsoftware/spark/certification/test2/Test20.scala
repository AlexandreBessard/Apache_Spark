  package com.sundogsoftware.spark.certification.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Test20 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("Alice", Seq(90, 85, 88)),
      ("Bob", Seq(92, 87)),
      ("Charlie", Seq(88, 90, 86))
    )

    // Define the schema
    val schema = List("name", "scores")

    // Create the DataFrame
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Explode the "scores" column into separate rows
    val explodedDF = df.withColumn("score", explode(col("scores")))

    // Show the exploded DataFrame
    explodedDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}
