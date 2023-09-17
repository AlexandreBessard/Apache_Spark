  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test7 {

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

    // Use withColumn to add a new column with a constant value "1"
    val resultDF: DataFrame = df.withColumn("new_column", lit(1))
    //OR
    val resultDF1: DataFrame = df.withColumn("new_column", lit("1"))
    resultDF1.show()

    // Show the result DataFrame
    resultDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}