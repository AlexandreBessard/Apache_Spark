package com.sundogsoftware.spark.certification.skillcertpro.test2

import breeze.linalg.max
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc, explode, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test18 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      ("HR", "Alice", Seq(90, 85, 88)),
      ("IT", "Bob", Seq(92, 87, 85)),
      ("HR", "Charlie", Seq(88, 90, 86))
    )

    // Define the schema
    val schema = List("department", "name", "score")

    // Create the DataFrame
    import spark.implicits._
    val peopleDF = data.toDF(schema: _*)

    // Define a Window specification
    val windowSpec = Window.partitionBy("department")
      .orderBy(desc("score"))

    // Perform the operations using expr
    val resultDF = peopleDF
      .withColumn("score", explode(col("score")))
      .select(
        col("department"),
        col("name"),
        // Create a column named ranked dense_rank(), but renamed rank with the alias.
        dense_rank().over(windowSpec).alias("rank"),
        expr("max(score) OVER (PARTITION BY department ORDER BY score DESC)")
          .alias("highest_score")
      )
      .where(col("rank") === 1)
      //.drop("rank")
      .orderBy("department")

    // Show the final result
    resultDF.show()

    // Explanation
    val resultDF2 = peopleDF.withColumn("score", explode(col("score")))
    resultDF2.show()

    val resultDF3 = resultDF2.select(col("department"), col("name"),
      dense_rank().over(windowSpec).alias("rank"))
    resultDF3.show()

    val resultDF4 = resultDF2.select(col("department"), col("name"),
      dense_rank().over(windowSpec).alias("rank"),
      expr("max(score) OVER (PARTITION BY department ORDER BY score DESC)")
    .alias("highest_score"))
    resultDF4.show()

    // Stop the SparkSession
    spark.stop()

  }
}
