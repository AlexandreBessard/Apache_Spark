package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSortedDataset {

  case class Book(value: String) //As to be named value

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // Read each line of my book into an Dataset
    import spark.implicits._
    val input = spark.read.text("data/book.txt") // DataFrame
      .as[Book] // DataSet

    //For debugging purpose
    input.explain();

    // Split using a regular expression that extracts words
    val words = input
      // split the value (column)
      .select(explode(split($"value", "\\W+")).alias("word")) // Every word has his own row.
      .filter($"word" =!= "") // Does not keep the space

    // Normalize everything to lowercase
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords.groupBy("word").count()

    // Sort by counts
    val wordCountsSorted = wordCounts.sort("count")

    // Show the results.
    wordCountsSorted.show(wordCountsSorted.count.toInt)

    // Better to use RDDs
    // ANOTHER WAY TO DO IT (Blending RDD's and Datasets)
    val bookRDD = spark.sparkContext.textFile("data/book.txt") // Returns a RDD, flat of text
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS() // Is is converted in DataSet
    println("Debug ==> ")
    wordsDS.explain()
    // Deal with colum name of "value"
    val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountsDS = lowercaseWordsDS.groupBy("word").count()
    val wordCountsSortedDS = wordCountsDS.sort("count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)

  }
}

