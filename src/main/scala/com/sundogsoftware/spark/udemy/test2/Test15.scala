import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType  // Add this import

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test15 {

  // Create case class with the schema of u.data
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test10") // Updated to match the object name
      .master("local[*]") // You can change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", 101),
      (2, "ProductB", 202),
      (3, "ProductC", 303)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "storeId")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Cast the "storeId" column to a string data type and select it
    // A number can be cast to a string
    val selectedDf = transactionsDf.select(col("storeId").cast(StringType))

    // Show the resulting DataFrame
    selectedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
