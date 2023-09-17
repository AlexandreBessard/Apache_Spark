  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types._
import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions.`collection asJava`


  object Test15 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrame
      val data = Seq(
        Row(1, "John"),
        Row(2, "Jane"),
        Row(3, "Bob")
      )

      // Define the initial schema
      val schema = StructType(Seq(
        // true means if nullable or not
        StructField("id", IntegerType, true),
        StructField("name", StringType, true)
      ))

      // Create a DataFrame
      import spark.implicits._
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Define a new column to be added
      // false means the value for this column can not be nullable.
      val newColumn = StructField("new_column", StringType, nullable = false)

      // Create a new schema by combining the existing schema with the new column
      val newSchema = StructType(schema.fields :+ newColumn)

      // Display the new schema
      println("New Schema:")
      newSchema.printTreeString()


      // Stop the SparkSession
      spark.stop()

    }
  }