import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Column}

object Test20 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UDFExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    import spark.implicits._

    // Define a Scala function
    def add_2_if_geq_3(x: Option[Int]): Option[Int] = {
      x match {
        case Some(value) if value >= 3 => Some(value + 2)
        case _ => x
      }
    }

    // Register the Scala function as a Spark UDF
    val add_2_if_geq_3_udf = udf(add_2_if_geq_3(_: Option[Int]): Option[Int])

    // Sample DataFrame
    val transactionsDf = Seq((1, 2), (2, 4), (3, null)).toDF("id", "predError")

    // Apply the UDF to the DataFrame
    val newDf = transactionsDf.withColumn("predErrorAdded", add_2_if_geq_3_udf(col("predError")))

    newDf.show()

    spark.stop()
  }
}
