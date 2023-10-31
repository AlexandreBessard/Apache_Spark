package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, callUDF, col, current_date, lit, row_number}

object Test6 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CustomerDataAnalysis")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    // TODO: need to be reviewed

    val employee = Seq(
      ("Jane", 30, "Sales", 4400),
      ("Alex", 32, "Sales", 4300),
      ("Serkan", 34, "IT", 5000),
      ("Philip", 33, "IT", 5500),
      ("Batu", 24, "Sales", 4350),
      ("Gerard", 27, "Director", 1000),
      ("Test", 27, "Test", 5000)
    )
    val employeeDF = spark.createDataFrame(employee)
      .toDF("Name", "Age", "Department", "Salary")
    /*
    This line defines a window specification for later use.
    It's saying that you want to partition (group) the data by
    the "Department" column and order the data within each partition
    by the "Salary" column in descending order. This will be used for
    ranking employees within their respective departments based on salary.
     */
    // because it is partitionBy, the orderBy is applied for each distinct partition.
    // desc: bigger to smaller, asc: smaller to bigger
    val window = Window.partitionBy("Department").orderBy(col("Salary").desc)
    import spark.implicits._
    //Find three maximum salary per department
    /*
    .withColumn("row", row_number.over(window)): This line adds a
    new column called "row" to the DataFrame.
    The "row_number.over(window)" function assigns a unique row number to each row within
    each department based on the specified window specification (ordering by salary).

    .where($"row" === 1): Here, you're filtering the DataFrame to only include rows where the "row" column is equal to 1. This effectively selects the employees with the highest salary in each department because of the earlier ordering.

    .drop("row"): Finally, you're removing the "row" column since it was only needed for ranking.

    .show(): This command displays the resulting DataFrame on the console, showing the employees with the highest salary in each department.
     */
    employeeDF
      .withColumn("row", row_number.over(window))
      .where($"row" === 1).drop("row").show()

    employeeDF
      .withColumn("row", row_number.over(window))
      .where(col("row") === 1).drop("row").show()

    //Explanation:
    println("row_number().over()")
    employeeDF.withColumn("row", row_number().over(window)).show();

    println("where()")
    employeeDF.where(col("Salary") === 4400).show()

    // Stop the SparkSession
    spark.stop()

  }
}
