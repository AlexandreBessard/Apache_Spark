import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._

object Test11 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      "1,2,3",
      "4,5,6",
      "7,8,9"
    )

    // Define a schema
    val schema = StructType(
      List(
        StructField("numbers", StringType, true)
      )
    )

    // Convert data to RDD[Row]
    val rowData = spark.sparkContext.parallelize(data.map(Row(_)))

    // Create DataFrame using schema
    val df = spark.createDataFrame(rowData, schema)

    // Show original DataFrame
    df.show()

    // Split the "numbers" column into multiple columns
    val splitCols = split(df("numbers"), ",")

    println("----> ")
    df.select(splitCols).show()

    df.select(splitCols.getItem(0)).show()

    // Add the split columns to the DataFrame
    val df2 = df.select(
      // 0 means the first column, index-based 0
      splitCols.getItem(0).as("num1"),
      splitCols.getItem(1).as("num2"),
      splitCols.getItem(2).as("num3")
    )

    // Show the new DataFrame
    df2.show()

    // Stop Spark session
    spark.stop()
  }
}
