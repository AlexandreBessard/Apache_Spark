package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, explode}

object Test18 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data: (itemId, attributes)
    val items = List(
      (1, Array("soft", "red", "cozy")),
      (2, Array("hard", "blue")),
      (3, Array("smooth", "green", "cozy", "large")),
      (4, Array("rough", "yellow"))
    )

    // Convert the list to a DataFrame
    import spark.implicits._
    val itemsDf = items.toDF("itemId", "attributes")

    // Filter, select, and explode
    val resultDf = itemsDf
      .filter(array_contains($"attributes", "cozy"))
      .select($"itemId", explode($"attributes").as("singleAttribute"))

    // Display the result
    resultDf.show()

    // Stop Spark session
    spark.stop()
  }
}
