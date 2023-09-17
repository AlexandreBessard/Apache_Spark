  package com.sundogsoftware.spark.certification.test3

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.functions.{col, date_add, to_date}
  import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types._

  import java.sql.Timestamp
  import scala.collection.convert.ImplicitConversions.`collection asJava`


  object Test29 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample data
      val data = Seq(
        Row("2023-09-10"),
        Row("2023-09-15"),
        Row("2023-09-20")
      )

      // Define the schema with a single "today" column
      val schema = StructType(Seq(StructField("today", StringType, false)))

      // Create a DataFrame
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)


      // Add a new column "week_later" by adding 7 days to "today"
      val resultDF = df
        .withColumn("week_later", date_add(col("today"), 7))

      // Show the result
      resultDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
