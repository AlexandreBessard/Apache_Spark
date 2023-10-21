package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, not}

object Test27 {

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
      (1, "itemA", "etX"),
      (2, "itemB", "exY"),
      (3, "itemC", "etZ"),
      (4, "itemD", "efW")
    )

    // Creating DataFrame
    val itemsDf = data.toDF("itemId", "itemName", "supplier")

    // Show original DataFrame
    println("Original DataFrame:")
    itemsDf.show()

    // Filtering rows where 'supplier' does not contain 'X' and selecting distinct "supplier"
    val filteredDf = itemsDf
      .filter(not(col("supplier").contains("X")))
      .select("supplier")
      .distinct()

    // Show filtered DataFrame
    println("Filtered DataFrame:")
    filteredDf.show()

    // Stop Spark session
    spark.stop()
  }
}
