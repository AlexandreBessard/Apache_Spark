package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, explode, count}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test21 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test21")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data for the DataFrame
    val data = Seq(
      ("Article1", Array("Tag1", "Tag2", "Tag3")),
      ("Article2", Array("Tag2", "Tag3")),
      ("Article3", Array("Tag1", "Tag4")),
      ("Article4", Array("Tag3", "Tag4", "Tag5"))
    )

    // Define the schema for the DataFrame
    val schema = List("articleId", "attributes")

    // Create a DataFrame from the sample data
    val articlesDf = spark.createDataFrame(data).toDF(schema: _*)

    // Explode the 'attributes' column into separate rows
    val explodedDf = articlesDf.select(explode(col("attributes")).alias("col"))

    explodedDf.show()

    // Group by the 'col' column and count occurrences
    // * is a special symbol that represents counting all rows in the specified column "col"
    val groupedDf = explodedDf.groupBy("col").agg(count("*").alias("count"))

    groupedDf.show()

    // Sort the DataFrame by 'count' in descending order and select the 'col' column
    val sortedDf = groupedDf.sort(desc("count")).select("col")

    // Show the resulting DataFrame
    sortedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
