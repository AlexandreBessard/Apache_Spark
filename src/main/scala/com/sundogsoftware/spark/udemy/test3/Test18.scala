import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, slice, split}

object Test18 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "Thick Coat for Walking in the Snow", "Sports Company Inc."),
      (2, "Elegant Outdoors Summer Dress", "YetiX"),
      (3, "Outdoors Backpack", "Sports Company Inc.")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Split the 'itemName' column by "-" or whitespace
    val separator = "\\s|-"

    // Add the 'itemNameBetweenSeparators' column to the DataFrame
    val resultDf = itemsDf
      .withColumn("itemNameBetweenSeparators",
        slice(split(itemsDf("itemName"), separator),
          // limit to 4 elements maximum per row
          1, 4))

    // Show the resulting DataFrame
    resultDf.show(truncate = false)

    // Stop the SparkSession
    spark.stop()
  }
}
