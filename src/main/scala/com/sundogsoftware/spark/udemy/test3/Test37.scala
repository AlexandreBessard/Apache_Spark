package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test37 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val employeeData = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Carol"),
      (4, "David")
    )

    val departmentData = Seq(
      (101, "HR"),
      (102, "Finance"),
      (103, "IT"),
      (1, "Sales")
    )

    // Define schemas
    val employeeSchema = List("emp_id", "emp_name")
    val departmentSchema = List("dept_id", "dept_name")

    // Create DataFrames
    val employeesDf: DataFrame = spark.createDataFrame(employeeData).toDF(employeeSchema: _*)
    val departmentsDf: DataFrame = spark.createDataFrame(departmentData).toDF(departmentSchema: _*)

    // 1. Inner Join
    val innerJoinedDf = employeesDf.join(departmentsDf, employeesDf("emp_id") === departmentsDf("dept_id"), "inner")
    println("Inner Join:")
    innerJoinedDf.show()

    // 2. Left Outer Join
    val leftOuterJoinedDf = employeesDf.join(departmentsDf, employeesDf("emp_id") === departmentsDf("dept_id"), "left_outer")
    println("Left Outer Join:")
    leftOuterJoinedDf.show()

    // 3. Right Outer Join
    val rightOuterJoinedDf = employeesDf.join(departmentsDf, employeesDf("emp_id") === departmentsDf("dept_id"), "right_outer")
    println("Right Outer Join:")
    rightOuterJoinedDf.show()

    // 4. Full Outer Join
    val fullOuterJoinedDf = employeesDf.join(departmentsDf, employeesDf("emp_id") === departmentsDf("dept_id"), "full_outer")
    println("Full Outer Join:")
    fullOuterJoinedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
