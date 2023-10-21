package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test24 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "itemA", "et"),
      (2, "itemB", "ex"),
      (3, "itemC", "et"),
      (4, "itemD", "ef")
    )

    val itemsDf = data.toDF("itemId", "itemName", "supplier")

    // Show original DataFrame
    println("Original DataFrame:")
    itemsDf.show()

    // Filter rows where supplier is 'et'
    val filteredDf = itemsDf.filter(col("supplier").isin("et"))

    // Show filtered DataFrame
    println("DataFrame after filtering:")
    filteredDf.show()

    // Stop Spark session
    spark.stop()
  }
}
