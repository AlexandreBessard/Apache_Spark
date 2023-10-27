package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc, explode}

object Test13 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //TODO: need to be reviewed

    // Sample data for the articlesDf DataFrame
    val data = Seq(
      (1, Array("blue", "winter", "cozy"), "Sports Company Inc."),
      (2, Array("red", "summer", "fresh", "cooling"), "YetiX"),
      (3, Array("green", "summer", "travel"), "Sports Company Inc.")
    )

    val articlesDf = data.toDF("itemId", "attributes", "supplier")

    // Explode the attributes column, group by each attribute, count, and order by count descending
    val resultDf = articlesDf
      .withColumn("attribute", explode($"attributes"))
      .groupBy("attribute")
      .agg(count($"attribute").alias("count"))
      .orderBy(desc("count"))

    val debug1 = articlesDf.withColumn("attribute", explode($"attributes"));
    debug1.show();

    val debug2 =
      debug1.groupBy("attribute")
        .agg(count("attribute").alias("count"));
    debug2.show()

    val debug3 = debug2.orderBy(desc("count"))
    debug3.show()


    // Display the result DataFrame
    resultDf.show()

    // Closing the SparkSession
    spark.close()
  }
}
