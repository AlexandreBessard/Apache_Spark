package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object Test14 {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Use all available cores on the local machine
      .getOrCreate()

    // Define a simple dataset
    val data = Seq(
      ("Alice", 34),
      ("Bob", 45),
      ("Cathy", 29),
      ("David", 31),
      ("Emily", 25)
    )

    // Define the schema
    val schema = StructType(List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    // Create a DataFrame
    /*
    This could happen because the createDataFrame method expects an RDD of Row
    objects when you provide a schema, and your RDD is currently a tuple (String, Int).
     You need to convert it to Row objects first.
     */
    val peopleDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data).map(t => Row(t._1, t._2)),
      schema
    )
    // Show the DataFrame
    peopleDF.show()

    /*
    Remember that take() and head() are actions that trigger the computation
    and retrieve the data to the driver node, whereas limit() is a transformation that
    returns a new DataFrame with restricted row count and does not trigger computation
     until an action is called on the resultant DataFrame.
     */
    val limitedDF = peopleDF.limit(3)
    println("Limit: --> ")
    limitedDF.show() // action

    /*
    The take(n: Int) action returns the first n rows of the DataFrame as an array.
    It's similar to limit(), but instead of returning a new DataFrame, it returns an array and
    thus collects the data to the driver node which may be problematic with large datasets.
     */
    val takenRows: Array[Row] = peopleDF.take(3)
    println("Take: --> ")
    takenRows.foreach(println)

    /*
    The head(n: Int) action is synonymous with take(n: Int), meaning it returns the
    first n rows as an array and sends the data to the driver node.
     If you use head() without a parameter, it returns the first row of the DataFrame.
     Without parameter, it retrieves the first row.
     */

    val headRows: Array[Row] = peopleDF.head(3)
    println("Head: --> ")
    headRows.foreach(println)

    // Stop the SparkSession
    spark.stop()
  }
}
