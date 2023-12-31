  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


  object Test26 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrames
      val personData = Seq(
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "David"),
        (7, "Clarisse"),
        (8, "Pascale")
      )

      val personColumns = Seq("personId", "personName")

      val df1Data = Seq(
        (1, "Apple"),
        (2, "Banana"),
        (3, "Cherry"),
        (5, "Grape"),
        (6, "Alex")
      )

      val df1Columns = Seq("itemId", "itemName")

      // Create DataFrames
      val person = spark.createDataFrame(personData).toDF(personColumns: _*)
      val df1 = spark.createDataFrame(df1Data).toDF(df1Columns: _*)

      // Define the join condition
      val joinExpression = df1("itemId") === person("personId")

      // Perform a right outer join between df1 and person
      /*
      "right_outer" join keeps all the rows from the right table and attaches
      any matching rows from the left table.
      If a match isn’t found, the left table's columns are filled with nulls.
      left table is df1 and right table is person
       */
      val result = df1.join(person, joinExpression, "right_outer")

      // Show the result
      result.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
