import breeze.numerics.pow
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Test5 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample DataFrame with a "predError" column
    val data = Seq((1, 2.5), (2, 3.0), (3, 1.5))
    val schema = List("id", "predError")
    val transactionsDF: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Calculate the square of "predError" using pow(col("predError"), 2)
    val squaredDF1 = transactionsDF
      .withColumn("predErrorSquared", org.apache.spark.sql.functions.pow(col("predError"), 2))

    // Calculate the square of "predError" using pow(col("predError"), lit(2))
    val squaredDF2 = transactionsDF
      .withColumn("predErrorSquared", org.apache.spark.sql.functions.pow(col("predError"), lit(2)))

    squaredDF1.show()
    squaredDF2.show()

    // Stop the SparkSession
    spark.stop()
  }
}
