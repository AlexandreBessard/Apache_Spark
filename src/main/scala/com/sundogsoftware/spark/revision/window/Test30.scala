package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test30 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "ItemA", "SupplierX"),
      (2, "ItemB", "SupplierY"),
      (3, "ItemC", "SupplierZ")
    )

    // Creating DataFrame from the sample data
    val itemsDf = data.toDF("itemId", "itemName", "supplier")

    // Location to save the Avro file
    val fileLocation = "path/to/your/output/directory"

    // Write DataFrame to Avro format
    itemsDf.write.format("avro").save(fileLocation)
    // DOEST NOT COMPILE
    //itemsDf.write.avro(fileLocation)

    println(s"Data written to $fileLocation in Avro format.")

    // Stop Spark session
    spark.stop()
  }
}
