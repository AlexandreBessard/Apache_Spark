package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test12 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    import spark.implicits._

    // Create the first DataFrame
    val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")

    // Create the second DataFrame
    val df2 = Seq((4, 5, 6)).toDF("col1", "LULU", "toto")

    // Union the two DataFrames
    val unionDF: DataFrame = df1.union(df2)

    // Show the result
    unionDF.show()


    // Stop the SparkSession
    spark.stop()
  }

}
