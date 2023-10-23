package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test16 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //TODO: need to be reviewed

    // Sample data
    val items = Seq((1, "A"), (2, "B"), (3, "C"))
    val transactions = Seq((1, 10), (2, 20), (2, 25), (3, 30), (4, 15))

    val itemsDf = items.toDF("itemId", "itemName")
    val transactionsDf = transactions.toDF("productId", "amount")

    // Joining the two DataFrames and dropping duplicates
    val resultDf = itemsDf
      .join(transactionsDf, itemsDf("itemId") === transactionsDf("productId"))
      .dropDuplicates("itemId") // takes string as parameter

    // distinct() method does not take any parameter.

    // Displaying the result
    resultDf.show()

    // Closing the SparkSession
    spark.close()
  }
}
