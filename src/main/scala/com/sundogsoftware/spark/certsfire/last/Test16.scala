package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct

object Test16 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder()
        .appName("YearDatasetExample")
        .master("local[*]") // Change to your cluster URL in a real cluster
        .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      ("A123", 1),
      ("A123", 2),
      ("B456", 3),
      ("B456", 4),
      ("C789", 5)
    )

    val df = data.toDF("invoiceNo", "Quantity")

    // Group by "invoiceNo" and count distinct values in "Quantity"
    val resultDf = df
      .groupBy("invoiceNo")
      .agg(countDistinct("Quantity").as("DistinctQuantityCount"))

    resultDf.show()

    // Stop the SparkContext
    spark.stop()
  }
}
