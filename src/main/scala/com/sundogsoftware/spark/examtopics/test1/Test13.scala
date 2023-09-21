package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Test13 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()


    /*
    Replace all null value from column Age to 0
     */

    // Define the schema for your DataFrame
    val schema = StructType(
      Seq(
        StructField("Name", StringType, nullable = true),
        StructField("Age", IntegerType, nullable = true),
        StructField("Location", StringType, nullable = true)
      )
    )

    // Create a DataFrame with sample data and apply the specified schema
    val data = Seq(
      Row("Alice", 25, "New York"),
      Row("Bob", null, null),
      Row(null, null, "San Francisco"),
      Row("David", null, "Los Angeles"),
      Row("Eve", 35, null)
    )

    val filledDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the resulting DataFrame
    filledDF.show()

    /*
    In summary, the coalesce function is used to handle
    missing or null values in a DataFrame column by providing
    a default value to replace those null values.
    It is a way to ensure that you have meaningful or usable data
    in place of nulls when necessary.
     */
    val filledDF1 = filledDF.withColumn("Age", coalesce(col("Age"), lit(0)))

    filledDF1.show()

    // Stop the SparkSession
    spark.stop()
  }

}
