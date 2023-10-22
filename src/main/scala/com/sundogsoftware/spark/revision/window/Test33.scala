package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, col}

object Test33 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    import spark.implicits._

    // Sample data: Let's assume that each item can have multiple attributes.
    val data = Seq(
      (1, "ItemA", Array("size-large", "color-red")),
      (2, "ItemB", Array("size-medium", "color-blue", "weight-light")),
      (3, "ItemC", Array("material-cotton", "color-green"))
    )

    val itemsDf = data.toDF("itemId", "itemName", "attributes")

    // Using the provided code snippet:
    val explodedDf = itemsDf.select(explode($"attributes").alias("attributes_exploded"))
      .filter(col("attributes_exploded").contains("i"))

    explodedDf.show()

    // Closing the SparkSession
    spark.close()
  }
}
