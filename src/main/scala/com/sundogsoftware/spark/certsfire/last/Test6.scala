package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col // Import Spark's built-in SQL functions

object Test6 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample scala list containing mixed types
    val throughputRates: List[Any] = List(123.45f, "abc", 456.78f, 789, "def", 101.11f)

    // Filter list to only include float-type values
    val floatRates: List[Float] = throughputRates.collect {
      case x: Float => x
    }

    // Convert filtered list to DataFrame
    val df =
      spark.createDataFrame(floatRates.map(Tuple1(_))).toDF("floatRates")

    // Display the resulting DataFrame
    df.show()
    
    spark.stop()
  }
}
