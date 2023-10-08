import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

// Define a case class to hold the word
case class Test(toto: String) // Create a column named "toto" in our case

object Test26 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      Test("apple"),
      Test("banana"),
      Test("cherry"),
      Test("date"),
      Test("elderberry"),
      Test("fig"),
      Test("grape"),
      Test("honeydew"),
      Test("honeydew")
    )

    // Create a DataFrame from the sequence of case class objects
    val df: DataFrame = spark.createDataFrame(data)

    // Group the DataFrame by the length of the "word" column and count occurrences
    val resultDF = df.groupBy("toto").count()

    // Show the resulting DataFrame
    resultDF.show()

    // Stop the SparkSession
    spark.stop()
  }
}
