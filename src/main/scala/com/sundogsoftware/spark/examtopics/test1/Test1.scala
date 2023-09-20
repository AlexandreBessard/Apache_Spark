package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object Test1 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()


    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create two DataFrames
    val studentsGrades: DataFrame = Seq(
      ("Alice", "Math", 90),
      ("Bob", "Math", 85),
      ("Alice", "History", 75),
      ("Charlie", "Math", 88)
    ).toDF("Name", "Subject", "Grade")

    val studentsActivities: DataFrame = Seq(
      ("Alice", "Chess"),
      ("Bob", "Drama"),
      ("Eve", "Chess"),
      ("Charlie", "Music")
    ).toDF("Name", "Activity")

    // Join the two DataFrames based on the "Name" column
    val joinedData = studentsGrades.join(studentsActivities, Seq("Name"), "inner")

    joinedData.show()

    // Stop the SparkSession
    spark.stop()
  }

}
