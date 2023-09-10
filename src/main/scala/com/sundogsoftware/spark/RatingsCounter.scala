package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter") //Parallelize on multiple CPUs core
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("data/ml-100k/u.data") //Load the data
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    // New RDD named ratings
    // Get the third field
    val ratings = lines.map(x => x.split("\t")(2)) // transform every row, field number 2 (0 based-index)
    
    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue() // Perform an action
    println(results) // Print out key value pairs
    
    // Sort the resulting map of (rating, count) tuples
    // Sort by the first column, (key) in increasing order
    // 1 represents the Key and 2 the value associated to that key.
    val sortedResults = results.toSeq.sortBy(_._1)
    
    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
