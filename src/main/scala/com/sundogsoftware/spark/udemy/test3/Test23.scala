package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test23 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data1 = Seq((1, "Alice"), (2, "Bob"))
    val data2 = Seq((3, "Charlie"), (4, "David"))

    // Define schemas for DataFrames with different column order
    val schema1 = List("id", "name")
    val schema2 = List("name", "id")

    // Create DataFrames from sample data
    val df1 = spark.createDataFrame(data1).toDF(schema1: _*)
    val df2 = spark.createDataFrame(data2).toDF(schema2: _*)

    // Perform unionByName
    val unionByNameDf: DataFrame = df1.unionByName(df2)

    val union: DataFrame = df1.union(df2);

    /*
    unionByName matches columns by name and requires the same column names and types,
    although they can appear in different orders.
     */
    println("----> unionByName: ")
    unionByNameDf.show()

    /*
    union matches columns by position and does not require the same column names or order.
     */
    println("----> union:")
    union.show()

    // Stop the SparkSession
    spark.stop()
  }
}
