package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, not}

object Test28 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      ("SupplierA", Array("Attr1", "Attr2")),
      ("SupplierB", Array("Attr3")),
      ("SupplierC", Array("Attr4", "Attr5", "Attr6"))
    )

    // Creating DataFrame
    val itemsDf = data.toDF("supplier", "attributes")

    // Register the DataFrame as a temporary view
    itemsDf.createOrReplaceTempView("itemsDf")

    // Use SQL query to select and explode the attributes column
    val explodedDf =
      spark.sql("FROM itemsDf SELECT supplier, explode(attributes) as attribute")

    // Show the result
    explodedDf.show()

    // Stop Spark session
    spark.stop()
  }
}
