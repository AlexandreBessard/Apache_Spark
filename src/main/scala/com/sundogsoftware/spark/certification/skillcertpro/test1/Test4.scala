package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test4 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CustomerDataAnalysis")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    val employee = Seq(
      ("Jane", 30, "Sales", 4400),
      ("Alex", 32, "Sales", 4300),
      ("Serkan", 34, "IT", 5000)
    )

    val employee1 = Seq(
      ("Jane", 30, "Sales", 4400),
      ("Alex", 32, "Sales", 4300),
      (null, 34, "IT", 5000)
    )
    val employeeDF3 = spark.createDataFrame(employee1).toDF


    // This syntax is correct
    val employeeDF2 = spark.createDataFrame(employee).toDF

    //Dropping rows containing any null values
    employeeDF3.na.drop().show()

    val employeeDF = spark.createDataFrame(employee)
      .toDF("Name", "Age", "Department", "Salary")

    // Both syntax are equivalents
    import spark.implicits._
    employeeDF.groupBy($"Department").avg("Salary").show()
    employeeDF.groupBy($"Department").agg(Map("Salary" -> "avg")).show()

    // Stop the SparkSession
    spark.stop()

  }
}
