package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Test15 {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession with custom driver memory setting
    val spark = SparkSession.builder()
      .appName("DriverMemoryExample")
      .master("local[*]")
      // This ensure that the driver has sufficient memory to perform its tasks
      .config("spark.driver.memory", "2g") // Set driver memory to 2 gigabytes
      .getOrCreate()

    // Sample DataFrame
    val data = Seq(
      ("John", "Doe", 28),
      ("Jane", "Smith", 34),
      ("Sam", "Brown", 52)
    )
    val columns = Seq("firstName", "lastName", "age")
    val df = spark.createDataFrame(data).toDF(columns: _*)

    df.show()

    // Stop Spark session
    spark.stop()
  }
}
