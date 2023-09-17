package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test12 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val simpleData = Seq(
      ("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )

    // Create a DataFrame from the sample data with column names
    val df1: DataFrame = spark.createDataFrame(simpleData)
      .toDF("employee_name", "department", "state", "salary", "age", "bonus")

    // Show the original DataFrame
    df1.show()

    // Group by the "department" column and calculate the sum of "salary" for each department
    val resultDf: DataFrame = df1
      .groupBy("department")
      .agg(sum("salary").alias("total_salary"))

    // Show the result
    resultDf.show()
    // Stop the SparkSession
    spark.stop()

  }
}
