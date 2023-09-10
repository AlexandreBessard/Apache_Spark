package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

/** Find the movies with the most ratings. */
object PopularMoviesDataset {

  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading u.data
    // Use to load our data using this schema since we do not have a header.
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    // Load up movie data as dataset
    val moviesDS = spark.read
      // separator a tab character.
      .option("sep", "\t")
      .schema(moviesSchema)
      // 196	242	3	881250949
      .csv("data/ml-100k/u.data") // DataFrame
      .as[Movie] // DataSet
    
    // Some SQL-style magic to sort all movies by popularity in one line!
    // group the movie ID. .count() function create a count column
    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count"))

    // Grab the top 10
    // Display the most popular movie meaning the most distinct rating associated to that movie.
    //topMovieIDs.show(topMovieIDs.count().toInt)
    topMovieIDs.show(10)

    // Stop the session
    spark.stop()
  }
  
}

