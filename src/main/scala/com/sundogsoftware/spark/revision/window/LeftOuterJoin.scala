package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LeftOuterJoin {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WindowFunctionsExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val employeesData = Seq(
      (1, "Alice", 100),
      (2, "Bob", 101),
      (3, "Charlie", 102),
      (4, "David", 103)
    )

    val departmentsData = Seq(
      (100, "HR"),
      (101, "Finance"),
      (102, "Engineering"),
      (104, "IT") // No correspondence
    )

    // Define DataFrames
    import spark.implicits._
    val employeesDF = employeesData
      .toDF("emp_id", "emp_name", "dept_id")

    val departmentsDF = departmentsData
      .toDF("dept_id", "dept_name")

    /*
    Take all records from employeesDF (the left DataFrame).
    For each record in employeesDF, find the corresponding match in departmentsDF based on the dept_id.
    If a match is found, combine the data from employeesDF and departmentsDF into a single record.
    If no match is found, fill in null for all columns from departmentsDF
     */
    // Left Outer Join
    val resultDF = employeesDF.join(departmentsDF, Seq("dept_id"),
      "left_outer")

    // Show the results
    resultDF.show()

    // Stop Spark session
    spark.stop()
  }
}
