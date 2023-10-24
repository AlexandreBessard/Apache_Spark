package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col // Import Spark's built-in SQL functions

object Test5 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for a DataFrame
    val data = Seq(
      (1, "item1"),
      (2, "item2"),
      (3, "item3"),
      (4, "item4"),
      (5, "item5")
    )

    val df = data.toDF("productId", "itemName")

    // Filter the DataFrame where productId is >= 2, and
    // then limit the result to 2 rows
      // limit returns a new Dataset
    val filteredDf = df.filter(col("productId") >= 2).limit(2)

    // Display the resulting DataFrame
    filteredDf.show()

    spark.stop()
  }
}
