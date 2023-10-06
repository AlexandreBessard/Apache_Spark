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

    // TODO: Need to be reviewed

    val employee1 = Seq(
      ("Jane", 30, "Sales", 4400),
      ("Alex", 32, "Sales", 4300),
      ("Serkan", 34, "IT", 5000)
    )

    val employee2 = Seq(
      ("Jane", 30, "Sales", 4400),
      ("Alex", 32, "Sales", 4300),
      (null, 34, "IT", 5000)
    )
    // This syntax is correct
    val employeeDF1 = spark.createDataFrame(employee1).toDF
    // By default, the column name will be _1, _2 and so on ....
    val employeeDF2 = spark.createDataFrame(employee2).toDF

    //Dropping rows containing any null values
    employeeDF2.na.drop().show()

    val employeeDF = spark.createDataFrame(employee1)
      .toDF("Name", "Age", "Department", "Salary")

    // Both syntax are equivalents
    import spark.implicits._
    employeeDF.groupBy($"Department").avg("Salary", "Age").show()
    employeeDF.groupBy($"Department").agg(Map("Salary" -> "avg")).show()

    // Stop the SparkSession
    spark.stop()

  }
}
