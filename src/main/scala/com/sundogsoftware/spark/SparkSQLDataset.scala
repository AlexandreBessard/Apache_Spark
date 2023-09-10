package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataset {

  // Defining a class. Define a Person object.
  // Contains 4 different fields.
  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use SparkSession interface
    // Open a session, close the session when done. else if can still running on our cluster.
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaPeople = spark.read // Read CSV file
      .option("header", "true") // Must match the header from the CSV and the Person field.
      .option("inferSchema", "true") // Must match the header from the CSV and the Person field.
      .csv("data/fakefriends.csv")
      //Could be removed and still works using DataFrame.
      .as[Person] // Takes Dataframe from CSV to Dataset (it is a Person)

    println("Schema -> ")
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people") // Create database named people

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect() // Collect the content
    
    results.foreach(println)
    
    spark.stop()
  }
}