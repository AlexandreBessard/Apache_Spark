package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, shuffle}

object Test7 {

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

    // Shows only the name, start at index-0 based
    employeeDF.foreach(f => println(f.get(0)))

    /*
    In summary, a partition in an RDD is a way to break up a
    large dataset into smaller pieces, allowing Spark to process
    the data efficiently in a distributed and parallel manner across a cluster of machines.
     */
    println("-> " + employeeDF.rdd.getNumPartitions)


    //Register the DataFrame as a SQL temporary view
    employeeDF.createOrReplaceTempView("people")
    val query = "SELECT NAME FROM PEOPLE"
    spark.sql(query).show()

    val test = employeeDF.repartition(5)

    employeeDF.show()

    test.show()

    //Renamed column
    // Call show() method else does not change the DataFrame because it is immutable.
    employeeDF.withColumnRenamed("Name", "Test").show();

    // Stop the SparkSession
    spark.stop()

  }
}
