package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test10 {

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
    val storesDF = spark.createDataFrame(storeData).toDF("storeId", "storeName")
    val employeesDF = spark.createDataFrame(employeeData).toDF("employeeId", "employeeName")

    // Cross Join DataFrames
    val crossJoinedDF = storesDF.crossJoin(employeesDF)

    // Display result
    crossJoinedDF.show()

    /*
    +-------+---------+----------+------------+
    |storeId|storeName|employeeId|employeeName|
    +-------+---------+----------+------------+
    |      1|   Store1|         1|       Alice|
    |      1|   Store1|         2|         Bob|
    |      1|   Store1|         3|     Charlie|
    |      2|   Store2|         1|       Alice|
    |      2|   Store2|         2|         Bob|
    |      2|   Store2|         3|     Charlie|
    |      3|   Store3|         1|       Alice|
    |      3|   Store3|         2|         Bob|
    |      3|   Store3|         3|     Charlie|
    +-------+---------+----------+------------+
     */

    // Stop Spark session
    spark.stop()
  }
}
