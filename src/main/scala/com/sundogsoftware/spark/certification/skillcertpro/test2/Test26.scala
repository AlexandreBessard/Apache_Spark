import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

// Define a case class to hold the word
case class Word(word: String)

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
      Word("apple"),
      Word("banana"),
      Word("cherry"),
      Word("date"),
      Word("elderberry"),
      Word("fig"),
      Word("grape"),
      Word("honeydew"),
      Word("honeydew")
    )

    // Create a DataFrame from the sequence of case class objects
    val df: DataFrame = spark.createDataFrame(data)

    // Group the DataFrame by the length of the "word" column and count occurrences
    val resultDF = df.groupBy("word").count()

    // Show the resulting DataFrame
    resultDF.show()

    // Stop the SparkSession
    spark.stop()
  }
}
