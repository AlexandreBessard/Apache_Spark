  package com.sundogsoftware.spark.certification.skillcertpro.test3

import breeze.linalg.sum
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.IntegerType


  object Test5 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val rawData = Seq(
      ("A", 20),
      ("B", 30),
      ("C", 80)
    )

    // Create a DataFrame with columns "Letter" and "Number"
    val df = spark.createDataFrame(rawData).toDF("Letter", "Number")

    // Calculate the sum of the "Number" column and store it in the 'result' variable
    val resultDF = df.groupBy().sum("Number")
    resultDF.explain()
    resultDF.show()
    /*
    (0): After calling collect(), you have an array of Row objects. (0) is used to access the first (and in this case, only)
    element of the array, which corresponds to the first row of your DataFrame.
    -------------------------
    .getLong(0): Finally, you are extracting a Long value from the first row of the DataFrame.
    The (0) indicates that you want to retrieve the value from the first (and only)
    column of the first row. In this case, it assumes that your DataFrame has only one column with Long values.
     */
    // First 0 means the first row and the second means the first column
    val result: Long = resultDF.collect()(0).getLong(0)

    // Print the sum of the "Number" column
    println(s"Sum of 'Number' column: $result")

    // Stop the SparkSession
    spark.stop()

  }
}