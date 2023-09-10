package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/** Find the superhero with the most co-appearances. */
object MostPopularSuperheroDataset {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MostPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ") // Space separator
      // 1 "24-HOUR MAN/EMMANUEL"
      .csv("data/Marvel-names.txt") // DataFrame
      .as[SuperHeroNames] // DataSet

    val lines = spark.read
      // It means 5988 has a connection with these other ids which represents a super hero
      // 5988 748 1722 3752 4655 5743 1872 3413 5527 6368 6085 4319 4728 1636 2397 3364 4001 1614 1819 1585 732 2660 3952 2507 3891 2070 2239 2602 612 1352 5447 4548 1596 5488 1605 5517 11 479 2554 2043 17 865 4292 6312 473 534 1479 6375 4456
      .text("data/Marvel-graph.txt")
      .as[SuperHero] // Plain all string DataSet

    // Default name of the column imported by the DataFrame named "value"
    lines.explain() // value -> String

    val connections = lines
      // Takes the first element of that string
      .withColumn("id", split(col("value"), " ") (0)) // Index based-0
      // Get the size of all elements and remove -1 because we do not count the first ID
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))



    val mostPopular = connections
      // Means connections column defined above.
        .sort($"connections".desc)
        .first()

    val mostPopularName = names // DataSet
      .filter($"id" === mostPopular(0))// (0) represents the ID with the most connections associated to it
      .select("name") // Get the name with the most connections
      .first()
    // (1) represents the number of connections
    println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopular(1)} co-appearances.")
  }
}
