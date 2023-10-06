import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.col

object Test32 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data (Replace with your actual data or DataFrame)
    val data = Seq(
      (1, "ProductA", 101),
      (2, "ProductB", 102),
      (3, "ProductA", 103),
      (4, "ProductC", 104),
      (5, "ProductB", 105)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "productName", "productId")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Filter rows where "productId" is greater than or equal to 2 and limit to 2 rows
    val filteredDf = transactionsDf.filter(col("productId") >= 2).limit(2)

    // Show the resulting DataFrame
    filteredDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
