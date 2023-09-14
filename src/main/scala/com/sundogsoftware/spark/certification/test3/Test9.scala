  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test9 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie"),
      (4, "David"),
      (5, "Eve")
    )

    val columns = Seq("ID", "Name")

    // Create a DataFrame
    import spark.implicits._
    val df = data.toDF(columns: _*)

    // Use withColumnRenamed to rename the "Name" column to "Full_Name"
    // old column name followed by the new column name
    val resultDF: DataFrame = df.withColumnRenamed("Name", "Full_Name")

    // Show the result DataFrame
    resultDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}