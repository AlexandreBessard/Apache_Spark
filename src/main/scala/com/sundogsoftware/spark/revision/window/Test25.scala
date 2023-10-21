package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test25 {

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
      (2, "itemB", null),
      (3, "itemC", "et"),
      (4, "itemD", "ef")
    )

    val itemsDf = data.toDF("itemId", "itemName", "supplier")

    // Show original DataFrame
    println("Original DataFrame:")
    itemsDf.show()

    // Repartition and write as CSV with options
    itemsDf
      .repartition(1) // 1. repartition to single partition
      .write
      .option("sep", ",") // 2 & 3. Set separator as comma
      .option("nullValue", "n/a") // 4. Set null values as "n/a"
      .csv("path/to/output/folder") // 5. Write out as CSV

    // Stop Spark session
    spark.stop()
  }
}
