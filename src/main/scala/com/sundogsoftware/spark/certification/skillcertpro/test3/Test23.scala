  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.functions.{col, to_date}
  import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types._
  import java.sql.Timestamp
  import scala.collection.convert.ImplicitConversions.`collection asJava`



  object Test23 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrame
      val data = Seq(
        Row("Alice", "New York", 30),
        Row("Bob", "San Francisco", 25),
        Row("Carol", "Los Angeles", 35),
        Row("David", "San Francisco", 28),
        Row("Eve", "New York", 32)
      )

      val schema = StructType(Seq(
        StructField("Name", StringType, nullable = false),
        StructField("City", StringType, nullable = false),
        StructField("Age", IntegerType, nullable = false)
      ))


      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // All of these methods are classified as "action"
      // Actions
      println("Count:")
      val rowCount = df.count()
      println(rowCount)

      println("\nShow:")
      df.show()

      println("\nCollect:")
      val rows = df.collect()
      rows.foreach(println)

      println("\nFirst:")
      val firstRow = df.first()
      println(firstRow)

      println("\nTake:")
      val n = 2
      val firstNRows = df.take(n)
      firstNRows.foreach(println)

      println("\nForEach:")
      df.foreach { row =>
        val name = row.getString(0)
        val city = row.getString(1)
        val age = row.getInt(2)
        println(s"Name: $name, City: $city, Age: $age")
      }


      // Stop the SparkSession
      spark.stop()

    }
  }
