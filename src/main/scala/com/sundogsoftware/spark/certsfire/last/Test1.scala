package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample DataFrames
    val transactionsData = Seq(
      (1, 1, 101),
      (2, 2, 101),
      (3, 2, 102),
      (4, 3, 101)
    )
    val transactionsDf = transactionsData.toDF("transactionId", "productId", "storeId")

    val itemsData = Seq(
      (1, "apple", "Fruit Vendor"),
      (2, "banana", "Tropical Fruits"),
      (3, "cherry", "Berry Supplier")
    )
    val itemsDf = itemsData.toDF("itemId", "itemName", "supplier")

    // Join the DataFrames as per the given conditions
    // Transaction with ID 3 does not match, because its storeId (102) matches the itemId of banana (2)
    val joinedDf = transactionsDf.join(itemsDf,
      transactionsDf("productId") === itemsDf("itemId") && transactionsDf("storeId") =!= itemsDf("itemId")
    )

    // Select desired columns from the joined DataFrame
    val resultDf = joinedDf.select("transactionId", "supplier")

    resultDf.show()

    spark.stop()
  }
}
