package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

object Test28 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("ReduceAndFoldExample")
      .master("local[*]")
      .getOrCreate()

    // Schema defined as a string
    val schemaString = "Id INT" // Column name and Type

    // Convert the schema string into actual Spark DataTypes using StructType
    val schema = StructType.fromDDL(schemaString)

    // Sample data
    val data = Seq(
      Row(1),
      Row(2),
      Row(3),
      Row(4),
      Row(5)
    )

    // Creating RDD of Rows
    val rdd = spark.sparkContext.parallelize(data)

    // Creating DataFrame using RDD and Schema
    val df = spark.createDataFrame(rdd, schema)

    // Show DataFrame
    df.show()



    // Stop the Spark session
    spark.stop()
  }
}
