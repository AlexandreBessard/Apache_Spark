package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test25 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test21")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Set the autoBroadcastJoinThreshold to 20 (in bytes)
    /*
    We set the spark.sql.autoBroadcastJoinThreshold configuration to 20.
    This means that Spark will automatically broadcast small DataFrames in join operations
     if their size is less than or equal to 20 bytes.
     Default value is 10
     */
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20)

    // Sample data for two DataFrames
    val employeesData = Seq(
      (1, "John"),
      (2, "Alice"),
      (3, "Bob")
    )

    val salariesData = Seq(
      (1, 50000),
      (2, 60000),
      (3, 55000)
    )

    // Create DataFrames from the sample data
    val employeesDF = spark.createDataFrame(employeesData).toDF("employeeId", "employeeName")
    val salariesDF = spark.createDataFrame(salariesData).toDF("employeeId", "salary")

    // Perform a join between the DataFrames
    /*
    We perform an inner join between the two DataFrames on the "employeeId" column. Since the DataFrames are small (less than or equal to 20 bytes),
    Spark will automatically broadcast them to optimize the join.
     */
    val joinedDF = employeesDF.join(salariesDF, "employeeId")

    // Show the result of the join
    joinedDF.show()


    // Stop the SparkSession
    spark.stop()
  }
}
