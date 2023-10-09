package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

import scala.util.Random

object Test28 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Random data from a DataFrame
     */

    // Sample data for transactionsDf
    // each map values is assigned to a column separate by a comma.
    val data = (1 to 20).map(i => (i, s"Product_$i", Random.nextDouble() * 1000))

    // Define the schema
    val schema = Seq(
      "transactionId", "product", "amount"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Specify the fraction of rows to sample (e.g., 0.5 for 50%)
    val sampleFraction = 0.5

    // Randomly sample about 10 rows (approximately 50% of the rows)
    /*
    With Replacement: If sampling is done with replacement,
    it means that each item that is randomly selected from the population
    is put back into the population before the next selection. In other words,
    the same item can be selected multiple times in the sample. This method allows
    for the possibility of selecting the same item more than once in the sample.

    Without Replacement: If sampling is done without replacement, it means
    that each item that is randomly selected from the population is not put
    back into the population before the next selection. Once an item is selected,
    it is removed from the population and cannot be selected again. This method
     ensures that each item appears only once in the sample.
     */
    // true means can appears multiple time.
    val sampledDf = transactionsDf.sample(withReplacement = true, fraction = sampleFraction, seed = 42)

    // Display the sampled DataFrame
    println("Sampled DataFrame:")
    sampledDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
