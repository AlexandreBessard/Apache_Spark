package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesNicerDataset {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    // Create a Map of Ints to Strings, and populate it from u.item.
    // Map movieID to movie name.
    var movieNames:Map[Int, String] = Map()
    // 1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()) {
      val fields = line.split('|')
      // ["1", "Toy Story (1995...", "01-Jan-1995", "", "http://us.imdb....", +19 more]
      if (fields.length > 1) {
        //Get the movie id as key and associate the key to the movie title.
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }

    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("PopularMoviesNicer")
      .master("local[*]")
      .getOrCreate()

    // broadcast -> sent out to all executors. Available to all executors.
    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    // Create schema when reading u.data
    val moviesSchema = new StructType()
    // Fields based on Movie class defined on top.
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    // Load up movie data as dataset
    import spark.implicits._
    val moviesDS = spark.read
      .option("sep", "\t") // Separator as tabulation
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data") // DataFrame
      .as[Movies] // DataSet

    // Get number of reviews per movieID
    // We also have a column named "count"
    val movieCounts = moviesDS.groupBy("movieID").count()
    movieCounts.printSchema()
    // Create a user-defined function to look up movie names from our
    // shared Map variable.

    // We start by declaring an "anonymous function" in Scala
    // Takes an Integer and return a String
    val lookupName : Int => String = (movieID:Int) => {
      // return the title associated to the movieID key
      nameDict.value(movieID)
    }

    // Then wrap it with a udf
    // user define function
    val lookupNameUDF = udf(lookupName) // use across our cluster

    // Add a movieTitle column using our new udf
    // Create a movieTile column
    /*
    |-- movieID: integer (nullable = true)
    |-- count: long (nullable = false)
     */
    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

    // Sort the results
    val sortedMoviesWithNames = moviesWithNames.sort("count")

    // Show the results without truncating it
    // truncate false else can be a weird display caused by the link
    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)
  }
}

