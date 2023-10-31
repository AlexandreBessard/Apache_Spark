package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test30 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample data for DataFrame "left"
    val dataLeft: Seq[(Int, String)] = Seq(
      (1, "Alice"),
      (2, "Bob")
    )
    val leftDF: DataFrame = dataLeft.toDF("ID", "Name")

    // Sample data for DataFrame "right"
    val dataRight: Seq[(Int, String)] = Seq(
      (101, "Apple"),
      (102, "Banana")
    )
    val rightDF: DataFrame = dataRight.toDF("ProductID", "Product")

    /*
    We use the crossJoin() method to perform a cross-join between "leftDF" and "rightDF."
    This operation combines every row from "leftDF" with every row from "rightDF,"
    resulting in a DataFrame where each row from "leftDF" is paired with every row from "rightDF."
     */
    // Perform a cross-join between "leftDF" and "rightDF"
    // Takes DataFrame as parameter
    val crossJoinedDF: DataFrame = leftDF.crossJoin(rightDF)

    // Show the resulting cross-joined DataFrame
    crossJoinedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
