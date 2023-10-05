import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Test29 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // Define the JSON file path
    val filePath = "/home/alex/Dev/Apache_Spark/SparkScalaCourse/src/main/scala/com/sundogsoftware/spark/udemy/test3/test29.json"

    // Define the JSON schema
    val jsonSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("country", StringType, nullable = true)
    ))

    // Read the JSON file with the specified schema
    val df = spark.read
      .option("multiline", "true") // Allows multiline JSON records
      .option("mode", "PERMISSIVE") // Sets the mode to permissive to handle bad records
      .schema(jsonSchema) // Specify the schema
      .json(filePath) // Read the JSON file

    // Show the DataFrame
    df.show()

    // Stop the SparkSession
    spark.stop()
  }
}
