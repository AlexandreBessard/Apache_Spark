package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col} // Import Spark's built-in SQL functions

object Test10 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed


    import spark.implicits._

    // List of all students
    val allStudents = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie"),
      (4, "David"),
      (5, "Eve")
    ).toDF("studentId", "studentName")

    // List of students who passed the exam
    val studentsWhoPassed = Seq(
      (1, "Passed"),
      (3, "Passed"),
      (5, "Passed")
    ).toDF("studentId", "status")

    // Using left_semi join to get the names of students who passed
    // Info: right_semi does not exist
    val studentsPassedNames =
      allStudents.join(studentsWhoPassed, allStudents("studentId") === studentsWhoPassed("studentId"), "left_semi")

    println("Students who passed the exam:")
    studentsPassedNames.show()

    spark.stop()
  }
}
