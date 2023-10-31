package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test14 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder()
        .appName("YearDatasetExample")
        .master("local[*]") // Change to your cluster URL in a real cluster
        .getOrCreate()

    // TODO: need to be reviewed


    // Sample data for storesDF
    val storesData = Seq(
      (1, "Store A"),
      (2, "Store B"),
      (3, "Store C")
    )

    // Sample data for employeesDF
    val employeesData = Seq(
      (101, "John", 1),
      (102, "Alice", 2),
      (103, "Bob", 1),
      (104, "Eve", 3)
    )

    import spark.implicits._

    // Create DataFrames from the sample data
    val storesDF = storesData.toDF("storeId", "storeName")
    val employeesDF = employeesData.toDF("employeeId", "employeeName", "storeId")

    // Perform the join operation
    // Merge the column store ID
    val joinedDF = storesDF.join(employeesDF, Seq("storeId"))
    // Does not merge the column "storeId"
    // Does not compile if both are in column instead of string when using Seq.
    val joinedDF1 = storesDF.join(employeesDF, Seq("storeId", "storeId"))

    // Show the resulting DataFrame
    joinedDF.show()
    joinedDF1.show()

    // Stop the SparkContext
    spark.stop()
  }
}
