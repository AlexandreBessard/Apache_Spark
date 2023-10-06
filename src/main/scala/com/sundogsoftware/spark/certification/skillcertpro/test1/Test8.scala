package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object Test8 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CustomerDataAnalysis")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    // TODO: need to be reviewed

    val employee = Seq(
      ("Jane", 30, "Sales", 4400),
      ("Alex", 32, "Sales", 4300),
      ("Serkan", 34, "IT", 5000),
      ("Philip", 33, "IT", 5500),
      ("Batu", 24, "Sales", 4350),
      ("Gerard", 27, "Director", 1000),
      ("Test", 27, "Test", 5000)
    )

    val newEmployee = Seq(
      ("Toto", 30, "Sales", 4400)
    )

    val employeeDF = spark.createDataFrame(employee)
      .toDF("Name", "Age", "Department", "Salary")
      //Save the table named t1
      .write.mode("overwrite").saveAsTable("t2")

    spark.sql("SELECT * FROM t2").show()

    val newEmployeeDF = spark.createDataFrame(newEmployee)
      .toDF("Name", "Age", "Department", "Salary")
      .write.mode("append").saveAsTable("t2")

    //Append the result to t1 with the new employee, save the table at this location:
    ///home/alex/Dev/Apache_Spark/SparkScalaCourse/spark-warehouse/t2
    spark.sql("SELECT * FROM t2").show()


    //----------------------------------
    // Sample large DataFrame
    val largeData = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie"),
      (4, "David")
    )
    val largeDF = spark.createDataFrame(largeData).toDF("key", "value")

    // Sample small DataFrame (to be broadcasted)
    val smallData = Seq(
      (1, "Sales"),
      (2, "Marketing")
    )
    val smallDF = spark.createDataFrame(smallData).toDF("key", "department")

    // Broadcast the small DataFrame
    /*
    Now, if the small set of data is really small and can easily fit in memory on each worker node,
    it's more efficient to send a copy of that small data to every worker.
    This way, each worker can independently match its portion of
    the big data with the small data without having to shuffle a lot of information
    between nodes. This process is called broadcasting.
     */
    val broadcastSmallDF = broadcast(smallDF)

    // Perform the join with broadcast
    val resultDF = largeDF.join(broadcastSmallDF, "key")
    //Same result as:
    val resultDF1 = largeDF.join(broadcast(smallDF), "key")

    // Show the result
    resultDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}
