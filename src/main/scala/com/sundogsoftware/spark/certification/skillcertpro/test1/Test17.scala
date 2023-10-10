package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object Test17 {

  // Define a case class to represent your data structure
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CustomerDataAnalysis")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    // Import implicits to leverage Spark's implicit conversions for Datasets
    import spark.implicits._

    // Create a sequence of Person objects
    val people = Seq(
      Person("John", 30),
      Person("Jane", 25),
      Person("Mike", 40)
    )

    // Convert the sequence of Person objects to a Dataset
    val ds: Dataset[Person] = people.toDS()

    // Perform a simple transformation using filter and map
    // Column name will be "value"
    ds.filter(_.age > 30).map(_.name).show()

    // SQL-like operations can also be performed on Datasets
    ds.createOrReplaceTempView("people")
    val sqlResult = spark.sql("SELECT * FROM people WHERE age > 30")
    sqlResult.show()

    // Stop the SparkSession
    spark.stop()

  }
}
