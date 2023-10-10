package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test16 {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession with custom deploy mode setting
    val spark = SparkSession.builder()
      .appName("DeployModeExample")
      /*
      when specifying a deploy mode, you generally want to target a cluster, so the master
      URL should point to your cluster manager (could be a Spark standalone cluster, Mesos, YARN, etc.).
      In this case, make sure to replace "spark://YOUR_SPARK_MASTER:7077" with your actual Spark master URL.
       */
      .master("spark://YOUR_SPARK_MASTER:7077") // Replace with your Spark master URL
      /*
      .config("spark.submit.deployMode", "client"): This line configures the Spark
      application to run in client deploy mode.
      If you want to run in cluster mode, you would replace "client" with "cluster".
       */
      .config("spark.submit.deployMode", "client")
      .getOrCreate()

    // Sample DataFrame
    val data = Seq(
      ("John", "Doe", 28),
      ("Jane", "Smith", 34),
      ("Sam", "Brown", 52)
    )
    val columns = Seq("firstName", "lastName", "age")
    val df = spark.createDataFrame(data).toDF(columns: _*)

    df.show()

    // Stop Spark session
    spark.stop()
  }
}
