  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast


  object Test31 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrames
      val dataA = Seq(
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol")
      )

      val dataB = Seq(
        (1, "HR"),
        (2, "Engineering"),
        (4, "Finance")
      )

      val columnsA = Seq("id", "name")
      val columnsB = Seq("id", "department")

      // Create DataFrames
      val dfA = spark.createDataFrame(dataA).toDF(columnsA: _*)
      val dfB = spark.createDataFrame(dataB).toDF(columnsB: _*)

      // Mark dfA for broadcast join
      /*
      Broadcasting allows the smaller DataFrame to be efficiently shared among all worker
      nodes in a Spark cluster, reducing data transfer and improving performance
       */
      val broadcastedDFA = broadcast(dfA)

      // Perform a join between dfB and broadcastedDFA on the "id" column
      val resultDF = dfB.join(broadcastedDFA, dfA("id") === dfB("id"))
      val resultDF1 = dfB.join(broadcastedDFA, dfB("id") === dfA("id"))
      val resultDF2 = broadcastedDFA.join(dfB, dfB("id") === dfA("id"))

      // Show the result
      resultDF.show()
      resultDF1.show()
      resultDF2.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
