import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test6 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    /*
    This code does not work
     */

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

    // Define the countToTarget UDF with an explicit return type
    val countToTarget = udf((target: Double) => {
      if (target.isNaN) {
        null.asInstanceOf[Array[Int]] // Handle the case when target is NaN
      } else {
        (0 until target.toInt).toArray // Convert Double to Int and generate an array from 0 to target-1
      }
    }, ArrayType(IntegerType))

    // Apply the UDF to the 'predError' column
    val resultDf = transactionsDf.withColumn("countToTarget", countToTarget(transactionsDf("predError")))

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
