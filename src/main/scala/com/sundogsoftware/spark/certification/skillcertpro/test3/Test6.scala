  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


  object Test6 {

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

    // Create a global temporary view
    // Lifetime of this table is tied to the spark application
    // Dropped when the application is terminated
    df.createOrReplaceGlobalTempView("my_view")

    // Create a temporary view named "my-view"
    df.createOrReplaceTempView("other_my_view")

    // Now, you can read the global temporary view
    // You must add the prefix "global_temp" if the table is created as GLOBAL temp view
    val readDF = spark.read.table("global_temp.my_view")

    // Now, you can use the temporary view in your Spark SQL queries
    // Do not need to add the prefix since it is a temp view
    spark.sql("SELECT * FROM other_my_view").show()

    // Show the contents of the DataFrame
    readDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}