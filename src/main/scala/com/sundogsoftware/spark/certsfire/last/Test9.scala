package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast // Import Spark's built-in SQL functions

object Test9 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    import spark.implicits._

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, "apple", 5.0),
      (2, "banana", 3.5),
      (3, "cherry", 7.0),
      (4, "date", 4.0)
    )

    val transactionsDf = transactionsData.toDF("transactionId", "itemName", "amount")

    // Sample data for itemsDf (Assume it's smaller and we want to filter transactionsDf by this list)
    val itemsData = Seq(
      (1, "apple"),
      (3, "cherry"),
      (5, "grape")
    )

    val itemsDf = itemsData.toDF("transactionId", "itemName")

    // Performing the left semi join using the provided instructions
    val resultDf =
      transactionsDf
        .join(broadcast(itemsDf), transactionsDf("transactionId"), "left_semi")

    /*
    In simple terms, a left_semi join in Spark (and in SQL in general) is like asking the question: "From my left dataset,
     which rows have a matching entry in the right dataset?"

    However, unlike other joins, you only get the rows from the left dataset. You won't see any columns
     from the right dataset in your result.

    Imagine you have a list of students in one dataset (left) and a list of students who passed an exam in another
    dataset (right). If you do a left_semi join with the student ID, the result will be a list of students (from the left dataset)
    who passed the exam, but it won't show any additional information about the exam from the right dataset.
    You just get to know who among your list passed the exam.
     */

    resultDf.show()


    spark.stop()
  }
}
