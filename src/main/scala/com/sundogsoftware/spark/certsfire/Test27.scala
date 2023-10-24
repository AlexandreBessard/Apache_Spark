package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split  // Corrected this import

object Test27 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    val data = Seq(
      (1, "apple-orange-banana-cherry-date"),
      (2, "fruit-veg-meat-dairy-bakery")
    )

    val df = data.toDF("id", "itemName")

    // Split the itemName column by '-' and select the 4th element (0-based index)
    val newDf =
      df.withColumn("itemNameBetweenSeparators",
        split($"itemName", "-").getItem(4)) // index-based 0

    newDf.show()

    spark.stop()
  }
}
