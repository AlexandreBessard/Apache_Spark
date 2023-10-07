package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object StringFunctions {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WindowFunctionsExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "John Smith"),
      (2, "Pamela Brown"),
      (3, "Larry White")
    )

    // Creating DataFrame
    val df = spark.createDataFrame(data).toDF("id", "name")

    // Various string functions

    // 1. Concatenate strings
    val withFullName = df.withColumn("full_name",
      concat(col("name"), lit(" is awesome!")))

    // 2. Upper
    val withUpperName = df.withColumn("upper_name",
      upper(col("name")))

    // 3. Substring
    val withSubstring = df.withColumn("first_name",
      /*
      Indices are 1-based, not 0-based. That means counting starts from 1.

      instr(name, ' ') finds the position of space: 5.
      instr(name, ' ') - 1 gives the length of the first name: 4.
      substring(name, 1, instr(name, ' ') - 1) extracts from the first
      character and takes 4 characters, resulting in: "John".
       */
      expr("substring(name, 1, instr(name, ' ') - 1)"))

    // 4. Replace
    val withReplacedName = df.withColumn("replaced_name",
      regexp_replace(col("name"), "John", "Jim"))

    // Show results
    withFullName.show(false)
    withUpperName.show(false)
    withSubstring.show(false)
    withReplacedName.show(false)

    // Stop Spark session
    spark.stop()
  }
}
