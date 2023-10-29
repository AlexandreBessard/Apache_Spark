package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, explode}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test22 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

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

    // Set the number of shuffle partitions to 100
    /*
    The line of code spark.conf.set("spark.sql.shuffle.partitions", 100)
    is used to configure the number of partitions that Spark should use when performing shuffle
    operations in Spark SQL. Shuffling is the process of redistributing data across partitions during
    certain operations like aggregations, joins, and groupings.

    We use spark.conf.set("spark.sql.shuffle.partitions", 100) to set the number of shuffle partitions to 100.
    This means that when Spark performs operations that require shuffling data, it will aim to create 100
    partitions for efficiency. You can adjust this value based on your cluster resources and workload.
     */
    spark.conf.set("spark.sql.shuffle.partitions", 100)

    // Load a sample DataFrame (replace this with your actual data)
    val data = Seq(("Alice", 25), ("Bob", 30), ("Carol", 28))

    val df = spark.createDataFrame(data).toDF("Name", "Age")

    // Perform a shuffle operation (e.g., grouping and aggregation)
    val resultDf = df.groupBy("Age").count()

    // Show the result
    resultDf.show()

    /*
    +---+-----+
    |Age|count|
    +---+-----+
    | 28|    1|
    | 25|    1|
    | 30|    1|
    +---+-----+
     */

    // Stop the SparkSession
    spark.stop()
  }
}
