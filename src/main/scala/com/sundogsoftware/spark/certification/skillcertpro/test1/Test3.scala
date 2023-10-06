package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CustomerDataAnalysis")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    val employee = Seq(
      ("Jane", 30, 1),
      ("Alex", 32, 1),
      ("Serkan", 34, 2)
    )

    val department = Seq(
      ("Sales", 1),
      ("IT", 2)
    )

    val employeeDF = spark.createDataFrame(employee)
      .toDF("Name", "Age", "DepartmentId")

    val departmentDF = spark.createDataFrame(department)
      .toDF("DepartmentName", "DepartmentId")

    // filter used to filter the rows
    employeeDF.filter("Age > 30")
      // join, add the table next to employeeDf with rows corresponding to the departmentId
      .join(departmentDF, employeeDF("DepartmentId") === departmentDF("DepartmentId"))
      // select the column
      .select("Name", "DepartmentName")
      .show()

    // Stop the SparkSession
    spark.stop()

  }
}
