package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object Test20 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Define the schema for the DataFrame
    val schema = StructType(List(
      StructField("season", StringType, true),
      StructField("wind_speed_ms", DoubleType, true)
    ))

    // Sample data to create the DataFrame
    val data = Seq(
      Row("Summer", 5.5),
      Row("Winter", 3.2),
      Row("Autumn", 4.1),
      Row("Spring", 6.0)
    )

    // Create the DataFrame using the defined schema and sample data
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Display the DataFrame
    df.show()

    // Stop Spark session
    spark.stop()
  }
}
