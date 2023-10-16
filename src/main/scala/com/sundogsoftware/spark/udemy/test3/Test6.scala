import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Test6 {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // Importing implicit encoders for standard library classes and tuples

    // Sample data
    val data = Seq(
      (1, 3.0),
      (2, 2.5),
      (3, 4.0)
    )

    // Creating a DataFrame using implicits
    val transactionsDf = data.toDF("transactionId", "predError")

    // Define the countToTarget UDF with an explicit return type
    val countToTarget = udf((target: Double) => {
      if (target.isNaN || target.isInfinity) {
        null.asInstanceOf[Array[Int]] // Handle the case when target is NaN or Infinity
      } else {
        // generate an array
        (0 until target.toInt).toArray // Convert Double to Int and generate an array from 0 to target-1
      }
    })

    // Apply the UDF to the 'predError' column
    val resultDf = transactionsDf
      .withColumn("countToTarget", countToTarget('predError))

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
