package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test21 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "John", "Smith", 1000),
      (2, "Jane", "Doe", 2000),
      (3, "Bob", "Johnson", 1500)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "firstName", "lastName", "amount")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Repartition the DataFrame to a single partition
    val repartitionedDf = transactionsDf.repartition(10)

    // Specify CSV options
    val csvPath = "/home/alex/Dev/Apache_Spark/SparkScalaCourse/src/main/scala/com/sundogsoftware/spark/revision/output"
    val csvOptions = Map(
      "sep" -> "\t", // Specify tab as the separator
      "nullValue" -> "n/a" // Specify "n/a" as the null value
    )

    // Write the DataFrame to CSV with the specified options
    repartitionedDf.write
      .options(csvOptions)
      .csv(csvPath)

    //OR

    repartitionedDf.write
      .option("sep", "\t") // Specify tab as the separator
      .option("nullValue", "n/a") // Specify "n/a" as the null value
      .csv(csvPath)

    // Stop the SparkSession
    spark.stop()
  }
}
