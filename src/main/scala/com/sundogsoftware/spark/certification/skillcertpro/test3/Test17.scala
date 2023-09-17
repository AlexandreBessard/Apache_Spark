  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.types._

  object Test17 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Define the schema
      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true)
      ))

      // Sample DataFrames
      val data1 = Seq(
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol")
      )

      val data2 = Seq(
        (4, "David"),
        (5, "Eve")
      )

      // Create DataFrames with the same schema
      val df1 = spark.createDataFrame(data1).toDF(schema.fieldNames: _*)
      val df2 = spark.createDataFrame(data2).toDF(schema.fieldNames: _*)

      // Union the two DataFrames
      val combinedDF: DataFrame = df1.union(df2)

      // Show the result
      combinedDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
