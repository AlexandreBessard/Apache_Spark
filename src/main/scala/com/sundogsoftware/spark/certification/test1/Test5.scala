package com.sundogsoftware.spark.certification.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, callUDF, current_date, lit}

object Test5 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CustomerDataAnalysis")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    val employee = Seq(
      ("Jane", 30, "Sales", 4400),
      ("Alex", 32, "Sales", 4300),
      ("Serkan", 34, "IT", 5000)
    )

    val employeeDF = spark.createDataFrame(employee)
      .toDF("Name", "Age", "Department", "Salary")

    // lit() create a column of literal values
    employeeDF.filter( employeeDF("Age") > lit(32)).show()

    //Num months add the number of month since the current date.
    employeeDF.withColumn("Date", add_months(current_date(), 100)).show();

    val values = Seq(
      ("id1", 1),("id2", 2), ("id3", 3)
    )

    val df = spark.createDataFrame(values).toDF("id", "value")
    /*
    (v: Int) => v * v: This part defines the actual UDF. In simple terms, it's a function that takes an
    integer v as input and returns the result of multiplying v by itself (i.e., squaring it).
    So, if you call simpleUDF(5) in your Spark SQL query, it will return 25 because 5 * 5 = 25.
     */
    // You create a function available in the entire cluster
    spark.udf.register("simpleUDF", (v: Int) => v * v)
    import spark.implicits._
    /*
    +---+----------------+
    | id|simpleUDF(value)|
    +---+----------------+
    |id1|               1|
    |id2|               4|
    |id3|               9|
    +---+----------------+
     */
    df.select($"id", callUDF("simpleUDF", $"value")).show()



    // Stop the SparkSession
    spark.stop()

  }
}
