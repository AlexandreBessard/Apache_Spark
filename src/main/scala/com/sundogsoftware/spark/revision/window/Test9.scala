package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month}

object Test9 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed


    // Sample data for stores and employees
    val storeData = Seq(
      (1, "Store1"),
      (2, "Store2"),
      (3, "Store3")
    )

    val employeeData = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    )

    // Create DataFrames
    val storesDF = spark.createDataFrame(storeData).toDF("id", "storeName")
    val employeesDF = spark.createDataFrame(employeeData).toDF("id", "employeeName")

    // Join DataFrames on 'id'
    val joinedDF = storesDF.join(employeesDF, Seq("id"))

    val joinedDF1 = storesDF.join(employeesDF, Seq("id", "id"))

    val storeData1 = Seq(
      (1, "Store1"),
      (2, "Store2"),
      (3, "Store3")
    )

    val employeeData1 = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    )

    val storesDF1 = spark.createDataFrame(storeData1).toDF("storeId", "storeName")
    val employeesDF1 = spark.createDataFrame(employeeData1).toDF("storeId", "employeeName")

    val joinedDF2 = storesDF1.join(employeesDF1, Seq("storeId", "storeId"))

    // Display result
    joinedDF.show()
    joinedDF1.show()
    joinedDF2.show()

    // Stop Spark session
    spark.stop()
  }
}
