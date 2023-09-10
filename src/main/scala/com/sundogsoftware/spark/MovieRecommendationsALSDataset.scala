package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.collection.mutable

object MovieRecommendationsALSDataset {

  case class MoviesNames(movieId: Int, movieTitle: String)
  // Row format to feed into ALS
  case class Rating(userID: Int, movieID: Int, rating: Float)

  // Get movie name by given dataset and id
  def getMovieName(movieNames: Array[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(_.movieId == movieId)(0)

    result.movieTitle
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Make a session
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local[*]")
      .getOrCreate()

    
    println("Loading movie names...")
    // Create schema when reading u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    // Create a broadcast dataset of movieID and movieTitle.
    // Apply ISO-885901 charset
    val names = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      // 1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    // copied in memory to scala
    val namesList = names.collect()

    // Load up movie data as dataset
    val ratings = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      // 196	242	3	881250949
      .csv("data/ml-100k/u.data")
      .as[Rating]
    
    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")
    
    val als = new ALS()
    // max iteration is set to 5
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")

    // Run that model, (black-box)
    val model = als.fit(ratings)
      
    // Get top-10 recommendations for the user we specified
    val userID:Int = args(0).toInt
    val users = Seq(userID).toDF("userID")
    println("Debug ===> ")
    users.printSchema()
    users.explain()
    // Top 10 recommendations
    val recommendations = model.recommendForUserSubset(users, 10)
    
    // Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")

    for (userRecs <- recommendations) {
      val myRecs = userRecs(1) // First column is userID, second is the recs
      // Cast the result as WrappedArray[Row]
      val temp = myRecs.asInstanceOf[mutable.WrappedArray[Row]] // Tell Scala what it is
      for (rec <- temp) {
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = getMovieName(namesList, movie)
        println(movieName, rating)
      }
    }
    
    // Stop the session
    spark.stop()

  }
}