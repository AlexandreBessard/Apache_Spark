package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}

object Test13 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Importing spark implicits
    import spark.implicits._

    val storesDF = Seq(
      (1, "StoreA", null),
      (2, null, "LocationB"),
      (3, "StoreC", "LocationC"),
      (4, null, null)
    ).toDF("storeId", "storeName", "storeLocation")

    storesDF.show()

    // Drop any row that contains at least one null value
    val droppedAnyDF = storesDF.na.drop()
    droppedAnyDF.show()

    /*
    Returns a new DataFrame that drops rows containing null or NaN values.
    If how is "any", then drop rows containing any null or NaN values.
    If how is "all", then drop rows only if every column is null or NaN for that row.
     */

    // Drop rows where all values are null
    val droppedAllDF = storesDF.na.drop("all")
    droppedAllDF.show()

    // Stop Spark session
    spark.stop()
  }
}
