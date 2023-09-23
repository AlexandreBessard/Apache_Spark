package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test23 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data
    val data: DataFrame = Seq(
      ("John", 25),
      ("Alice", 30),
      ("Bob", 22),
      ("David", 35),
      ("Eve", 28)
    ).toDF("Name", "Age")

    // Define a typed Scala UDF with input and output types
    //output type is a string
    val ageDescriptionUDF = udf((age: Int) => s"$age years old")

    // Register the UDF with Spark
    spark.udf.register("age_description", ageDescriptionUDF)

    // Use the UDF in a SQL query
    data.createOrReplaceTempView("people")
    val result = spark.sql("SELECT Name, age_description(Age) AS AgeDescription FROM people")

    // Show the result
    result.show()

    // Stop the SparkSession
    spark.stop()
  }

}
