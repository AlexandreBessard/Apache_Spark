package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}

object Test30 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Define a case class for your data
    case class Transaction(transactionId: Int, predError: Option[Int], value: Option[Int], storeId: Option[Int], productId: Int, f: Option[Int])

    val transactionsData = Seq(
      Transaction(1, Some(3), Some(4), Some(25), 1, None),
      Transaction(2, Some(6), Some(7), Some(2), 2, None),
      Transaction(3, Some(3), None, Some(25), 3, None),
      Transaction(4, None, None, Some(3), 2, None),
      Transaction(5, None, None, None, 2, None),
      Transaction(6, Some(3), Some(2), Some(25), 2, None)
    )

    //val transactionsDs = transactionsData.toDF()

/*    val resultDf = transactionsDs
      .filter($"storeId".isNotNull && ($"productId" === 2 || $"productId" === 3))
      .groupBy("storeId", "productId")
      .agg(avg(col("predError")).alias("mean_predError"))
      .orderBy(col("storeId"))

    resultDf.show()

*/

    spark.stop()
  }
}
