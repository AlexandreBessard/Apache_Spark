package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.col
    
object DataFramesDataset {
  
  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    //DataFrame
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person] // Becomes a DataSet.

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    people.printSchema()
    
    println("Let's select the name column:")
    people.select("name").show()
    
    println("Filter out anyone over 21:")
    // age is the name of the columny
    people.filter(people("age") < 21).show()
   
    println("Group by age:")
    people.groupBy(col("age")).count().show()
    // Same result
    people.groupBy("age").count().show()


    println("Make everyone 10 years older:")
    //name colum and age column, it shows these 2 columns with (age + 10) column.
    people.select(people("name"), people("age") + 10).show()
    
    spark.stop() //Never forget to stop a session when done.
  }
}