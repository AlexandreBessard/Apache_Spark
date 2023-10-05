import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

object Test30 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("TestUDF")
      .master("local[*]")
      .getOrCreate()

    // Sample data (Replace with your actual data or DataFrame)
    val data = Seq(
      (1, 3.0),
      (2, 2.5),
      (3, 4.0)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "predError")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Define the UDF without specifying the return type parameter
    val toLimit = udf((value: Double) => if (value > 3.0) 3.0 else value)

    // Register the UDF
    spark.udf.register("LIMIT_FCN", toLimit)

    // Create a temporary view of the DataFrame
    transactionsDf.createOrReplaceTempView("transactionsDf")

    // Use the UDF in a SQL query
    val resultDf = spark
      .sql("SELECT transactionId, LIMIT_FCN(predError) AS result FROM transactionsDf")

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
