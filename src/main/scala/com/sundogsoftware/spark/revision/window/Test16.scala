package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, explode, col}

object Test16 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data: (userId, hobbies)
    val data = List(
      (1, "reading,swimming,biking"),
      (2, "hiking,reading"),
      (3, "swimming,biking")
    )

    val hobbiesDF = data.toDF("userId", "hobbies")

    // Use split to tokenize the comma-separated strings into arrays
    val splitDF = hobbiesDF
      .withColumn("hobbyArray", split(col("hobbies"), ","))

    // Display split results
    splitDF.show()

    // Use explode to transform arrays into separate rows
    // TODO: warning, explode method should be used from a select method
    val explodedDF = splitDF
      .select(col("userId"), explode(col("hobbyArray"))
        .as("hobby"))

    // Display explode results
    explodedDF.show()

    // Stop Spark session
    spark.stop()
  }
}
