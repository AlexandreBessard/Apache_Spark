import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, length, lower, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test27 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test21")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "Thick Coat for Walking in the Snow", Seq("blue", "winter", "cozy"), "Sports Company Inc."),
      (2, "Elegant Outdoors Summer Dress", Seq("red", "summer", "fresh", "cooling"), "YetiX"),
      (3, "Outdoors Backpack", Seq("green", "summer", "travel"), "Sports Company Inc.")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "attributes", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // length: Computes the character length of a given string or number of bytes of a binary string
    val consonantCountDf = itemsDf.select(
      length(regexp_replace(lower(col("itemName")), "a|e|i|o|u|\\s", "")).alias("consonant_ct")
    )

    /*
    Regexp Replacement:

    "a|e|i|o|u|\\s": This regular expression pattern targets all vowel characters
    ("a", "e", "i", "o", "u") and whitespace characters (\\s).
    "": Replaces all instances of the matched pattern with an empty string,
    effectively removing them from the string.
     */

    val consonantCountDf1 = itemsDf.select(
      regexp_replace(lower(col("itemName")), "a|e|i|o|u|\\s", "").alias("consonant_ct")
    )

    // Show the resulting DataFrame
    consonantCountDf.show()
    consonantCountDf1.show()

    // Stop the SparkSession
    spark.stop()
  }
}
