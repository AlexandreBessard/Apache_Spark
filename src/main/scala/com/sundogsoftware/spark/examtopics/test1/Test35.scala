package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test35 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Importing implicits is needed to use methods like toDF()
    import spark.implicits._

    // Define two DataFrames with different column order
    val df1 = Seq(
      (1, "foo"),
      (2, "bar"),
      (3, "baz")
    ).toDF("id", "value")

    val df2 = Seq(
      ("qux", 4),
      ("quux", 5),
      ("quuz", 6)
    ).toDF("value", "id")

    // Use unionByName to union them
    val unionDF = df1.unionByName(df2)

    // Show the result
    unionDF.show()

    // Throws an exception if a column name mismatched with the other one.
    val df3 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
    val df4 = Seq((4, 5, 6)).toDF("col1", "col2", "col0")
    df3.unionByName(df4).show

    // Stop the SparkSession
    spark.stop()
  }

}
