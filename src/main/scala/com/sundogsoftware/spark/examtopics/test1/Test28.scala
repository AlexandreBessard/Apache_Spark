package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, dayofyear}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test28 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample data for stores
    val storesData: Seq[(Int, String)] = Seq(
      (1, "Store A"),
      (2, "Store B"),
      (3, "Store C")
    )
    val storesDF: DataFrame = storesData.toDF("StoreId", "StoreName")

    // Sample data for employees
    val employeesData: Seq[(Int, String, Int)] = Seq(
      (101, "Employee 1", 1),
      (102, "Employee 2", 2),
      (103, "Employee 3", 3),
      (104, "Employee 4", 1)
    )
    val employeesDF: DataFrame = employeesData.toDF("EmployeeId", "EmployeeName", "StoreId")

    // Inner join storesDF and employeesDF on the "StoreId" column
    val joinedDF: DataFrame = storesDF.join(employeesDF, Seq("StoreId"), "inner")

    // Show the resulting joined DataFrame
    joinedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
