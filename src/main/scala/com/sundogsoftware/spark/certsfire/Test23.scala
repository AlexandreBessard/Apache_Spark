package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object Test23 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, 10.5, 101, "attr1"),
      (2, 20.0, 102, "attr2"),
      (3, 15.0, 101, "attr3"),
      (4, 30.5, 103, "attr4")
    )

    val transactionsDf = transactionsData.toDF("productId", "value", "storeId", "attributes")

    // Sample data for itemsDf
    val itemsData = Seq(
      (1, "apple"),
      (2, "banana"),
      (3, "cherry"),
      (5, "date")
    )

    val itemsDf = itemsData.toDF("itemId", "itemName")

    // Creating temporary views
    transactionsDf.createOrReplaceTempView("transactionsDf")
    itemsDf.createOrReplaceTempView("itemsDf")

    // SQL statement to join the two DataFrames
    val statement =
      """
  SELECT * FROM transactionsDf
  INNER JOIN itemsDf
  ON transactionsDf.productId=itemsDf.itemId
"""

    // Executing the SQL statement and dropping some columns
    val resultDf = spark.sql(statement).drop("value", "storeId", "attributes")

    // Showing the result
    resultDf.show()
    spark.stop()
  }
}
