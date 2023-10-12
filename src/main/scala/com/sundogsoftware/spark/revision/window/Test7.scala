package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object Test7 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // Enable implicit conversions
    import spark.implicits._

    // Create a DataFrame
    val data = Seq(
      ("Store1", 10),
      ("Store2", 85),
      ("Store3", 50),
      ("Store4", 95),
      ("Store5", 15)
    )

    val storesDF = spark.createDataFrame(data).toDF("store", "customerSatisfaction")

    // Define a UDF
    val assessPerformanceUDF = udf((customerSatisfaction: Integer) => {
      customerSatisfaction match {
        case x if x < 20 => 1
        case x if x > 80 => 3
        case _ => 2
      }
    })

    // Use UDF to create a new column in DataFrame
    val resultDF = storesDF
      .withColumn("result", assessPerformanceUDF(col("customerSatisfaction")))

    // Display the result
    resultDF.show()

    // Stop Spark session
    spark.stop()
  }
}
