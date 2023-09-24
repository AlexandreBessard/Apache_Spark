package com.sundogsoftware.spark.revision

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, substring_index}

object Test2 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    /*
    Example of transformation functions
     */

    // Sample employee DataFrame
    val employees = Seq(
      ("John", "Engineering", 30),
      ("Alice", "Sales", 28),
      ("Bob", "Engineering", 35),
      ("Eve", "HR", 32)
    )
    val employeeDF =
      employees.toDF("name", "department", "age")

    // Sample department DataFrame
    val departments = Seq(
      ("Engineering", "San Francisco"),
      ("Sales", "New York"),
      ("HR", "Chicago")
    )
    val departmentDF = departments.toDF("department", "location")

    // Transformation 1: Select columns
    val selectedDF = employeeDF.select("name", "age")

    // Transformation 2: Filter rows
    val filteredDF = employeeDF.filter(col("age") > 30)
    println("filter ->")
    filteredDF.show()

    // Transformation 3: Group by department and calculate average age
    val avgAgeByDeptDF = employeeDF.groupBy("department")
      .agg(avg("age").alias("avg_age"))

    // Transformation 4: Add a new column for department location
    val joinedDF = employeeDF.join(departmentDF, Seq("department"), "left")
      .withColumnRenamed("location", "dept_location")

    // Show the results of transformations
    println("Selected Columns:")
    selectedDF.show()

    println("Filtered Rows (age > 30):")
    filteredDF.show()

    println("Average Age by Department:")
    avgAgeByDeptDF.show()

    println("Joined Employee and Department DataFrames:")
    joinedDF.show()
    // Stop the SparkSession
    spark.stop()
  }

}
