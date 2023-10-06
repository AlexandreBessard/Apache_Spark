import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test36 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data for transactionsDf
    val transactionData = Seq(
      (1, "ProductA", 101),
      (2, "ProductB", 102),
      (3, "ProductA", 103),
      (4, "ProductC", 104),
      (5, "ProductB", 105)
    )

    // Sample data for itemsDf
    val itemData = Seq(
      (101, "Thick Coat for Walking in the Snow"),
      (102, "Elegant Outdoors Summer Dress"),
      (103, "Outdoors Backpack")
    )

    // Define the schemas for the DataFrames
    val transactionSchema = List("transactionId", "productName", "productId")
    val itemSchema = List("productId", "itemName")

    // Create DataFrames from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(transactionData).toDF(transactionSchema: _*)
    val itemsDf: DataFrame = spark.createDataFrame(itemData).toDF(itemSchema: _*)

    // Perform a left semi-join with broadcasting
    val resultDf = transactionsDf
      .join(broadcast(itemsDf), Seq("productId"), "left_semi")

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
