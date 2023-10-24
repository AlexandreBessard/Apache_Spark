package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}  // Corrected this import

object Test28 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "Basketball", "Sports Unlimited", Array("round", "orange")),
      (2, "Banana", "Fruit Corp", Array("yellow", "curved")),
      (3, "Tennis Ball", "Sports Palace", Array("round", "green")),
      (4, "Bread", "Bakery Central", Array("brown", "soft"))
    )

    val df = data.toDF("itemId", "itemName", "supplier", "attributes")

    // Filter rows where supplier contains "Sports" and explode the "attributes" column
    val filteredAndExplodedDf =
      df.filter(col("supplier").contains("Sports"))
      .withColumn("attribute", explode(col("attributes")))
        // can NOT mix string and col in the select() method
      .select(col("itemName"), col("attribute"))

    // Show the result
    filteredAndExplodedDf.show()

    spark.stop()
  }
}
