package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test41 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample sales data
    val salesData = Seq(
      ("John", "January", 1000),
      ("John", "February", 1100),
      ("Jane", "January", 1200),
      ("Jane", "March", 1300),
      ("Jake", "February", 1400),
      ("Jake", "April", 1500)
    )

    val df = spark.createDataFrame(salesData)
      .toDF("Salesperson", "Month", "Amount")

    df.show()

    /*
    +----------+--------+------+
    |Salesperson|   Month|Amount|
    +----------+--------+------+
    |      John| January|  1000|
    |      John|February|  1100|
    |      Jane| January|  1200|
    |      Jane|   March|  1300|
    |      Jake|February|  1400|
    |      Jake|   April|  1500|
    +----------+--------+------+
     */

    // Pivot the DataFrame on the "Month" column
    val pivotedDF = df.groupBy("Salesperson")
      .pivot("Month")
      .sum("Amount")

    pivotedDF.show()

    /*
    +----------+-------+--------+------+-----+
    |Salesperson|  April|February|January|March|
    +----------+-------+--------+------+-----+
    |      John|   null|    1100|  1000| null|
    |      Jake|   1500|    1400|  null| null|
    |      Jane|   null|    null|  1200| 1300|
    +----------+-------+--------+------+-----+
     */


    // Stop the SparkSession
    spark.stop()
  }
}
