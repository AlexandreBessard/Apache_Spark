  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random


  object Test10 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie"),
      (4, "David"),
      (5, "Eve")
    )

    val columns = Seq("ID", "Name")

    // Create a DataFrame (replace this with your actual DataFrame)
    import spark.implicits._
    val df = data.toDF(columns: _*)

    // Define the fraction to sample (25%)
    val fractionToSample = 0.25

    // Use the sample method to create a new DataFrame with 25% random records with replacement
    val sampledDF: DataFrame = df. // Create a sample using the DataFrame "df"
    // sample row from DataFrame
    // Same row can be sample multiple time. (withReplacement)
    // 0.25 -> 25% of the rows will be sample
    /*
    The seed parameter allows you to specify a random seed for reproducibility.
    When you provide a specific seed value (in this case, generated using Random.nextLong()),
    the random sampling process will be deterministic and yield the same results
    each time you run the code with the same seed. This can be useful for debugging and
    ensuring consistent results when working with random data.
     */
      sample(withReplacement = true, fraction = fractionToSample, seed = 5)

    // Show the sampled DataFrame
    sampledDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}