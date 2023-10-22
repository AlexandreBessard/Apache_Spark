package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.BooleanType

object Test6 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample function for demonstration
    def evaluateTestSuccess(storeId: Int): Boolean = {
      storeId match {
        case x if x > 50 => true
        case _ => false
      }
    }

    // Registering the UDF
    val evaluateTestSuccessUDF = functions.udf(evaluateTestSuccess(_: Int): Boolean)

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, 30),
      (2, 60),
      (3, 45),
      (4, 100)
    )

    val transactionsDf = transactionsData.toDF("id", "storeId")

    // Using the UDF to transform the DataFrame
    val transformedDf = transactionsDf
      .withColumn("result", evaluateTestSuccessUDF(col("storeId")))

    // Display the transformed DataFrame
    transformedDf.show()

    // Closing the SparkSession
    spark.close()
  }
}
