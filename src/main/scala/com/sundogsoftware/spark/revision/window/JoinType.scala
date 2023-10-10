package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object JoinType {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WindowFunctionsExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Creating a DataFrame of employees
    val employees = Seq(
      (1, "Alice", "HR"),
      (2, "Bob", "Engineering"),
      (3, "Charlie", "Finance"),
      (4, "Diana", "HR"),
      (5, "noMatch", "noMatch")
    ).toDF("empId", "name", "department")

    // Creating a DataFrame of departments
    val departments = Seq(
      ("HR", "Human Resources"),
      ("Engineering", "Engineering Department"),
      ("Finance", "Finance Department"),
      ("Marketing", "Marketing Department"),
      ("toto", "toto")
    ).toDF("deptName", "fullName")

    // Example of an Inner Join
    val innerJoinResult: DataFrame = employees.join(departments,
      employees("department") === departments("deptName"),
      "inner")
    println("Inner Join Result:")
    innerJoinResult.show()

    // Example of a Left Outer Join
    val leftOuterJoinResult: DataFrame = employees.join(departments,
      employees("department") === departments("deptName"),
      "left_outer") // Memo technique: put null on the LEFT if no match
    println("Left Outer Join Result:")
    leftOuterJoinResult.show()

    // Performing a Left Join
    // No differences between left and left_outer join
    val leftJoinResult: DataFrame = employees.join(departments,
      employees("department") === departments("deptName"),
      "left")
    println("Left Join Result:")
    leftJoinResult.show()

    // Stop Spark session
    spark.stop()
  }
}
