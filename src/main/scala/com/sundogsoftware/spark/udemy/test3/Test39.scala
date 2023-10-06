import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object Test39 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, Array("Attribute1"), "SupplierA"),
      (2, Array("Attribute2"), "SupplierB"),
      (3, Array("Attribute3"), "SupplierC"),
      (4, Array("Attribute4"), "SupplierD")
    )

    // Define the schema
    val schema = new StructType()
      .add(StructField("itemId", IntegerType, true))
      .add(StructField("attributes", ArrayType(StringType, true), true))
      .add(StructField("supplier", StringType, true))

    // Create a DataFrame with the specified schema
    val rows = data.map { case (itemId, attributes, supplier) =>
      Row(itemId, attributes, supplier)
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows), schema)

    // Specify the directory where you want to save the Parquet file
    val outputPath = "/home/alex/Dev/Apache_Spark/SparkScalaCourse/src/main/scala/com/sundogsoftware/spark/udemy/test3/DataStore/sample_723.parquet"

    // Write the DataFrame to Parquet format
    df.write.mode("overwrite").parquet(outputPath)

    // Show the resulting DataFrame
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}
