package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object Test29 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    /*
    Without DPP:
    You take a book from Room B, note down the author's name.
    You walk into Room A, find all the books by that author, and keep them aside.
    Repeat this for every book in Room B, even if you’ve already searched for that author before.
    This method is not efficient because you may visit the same shelves in Room A multiple times.

    With DPP:
    You check all the books in Room B first and make a unique list of authors.
    Then you walk into Room A, find all the books by those authors in a single go.

    Without DPP: Spark might scan the entire large DataFrame to find matching records for every record
    in the smaller DataFrame.

    With DPP: Spark smartly identifies the relevant partitions (like the unique list of authors) in the
    larger DataFrame to scan, based on the filter condition applied to the smaller DataFrame.
    It only scans relevant portions of the large DataFrame, hence saving processing time and resources.

    Benefits
    Efficiency: Only the necessary data partitions are read, reducing I/O and computation.
    Speed: Faster query execution as it avoids scanning irrelevant data.
    Resource Saving: Consumes less computational and memory resources.
    In summary, enabling DPP in Apache Spark helps in smartly reducing the amount of data
     that needs to be read and processed during join operations, particularly when dealing with
     large partitioned tables, resulting in more efficient and faster query execution.
     */

    val spark = SparkSession.builder()
      .appName("DPP Example")
      .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    // Example schema and data for customers DataFrame
    val customerSchema = StructType(Array(
      StructField("customer_id", IntegerType),
      StructField("customer_name", StringType)
    ))

    val customerData = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
      Row(3, "Charlie")
    )

    val customers = spark.createDataFrame(
      spark.sparkContext.parallelize(customerData),
      customerSchema
    )

    // Example schema and data for transactions DataFrame
    val transactionSchema = StructType(Array(
      StructField("transaction_id", IntegerType),
      StructField("customer_id", IntegerType),
      StructField("transaction_date", StringType),  // Keep it simple with StringType for date
      StructField("transaction_amount", DoubleType)
    ))

    val transactionData = Seq(
      Row(1, 1, "2023-01-01", 100.0),
      Row(2, 2, "2023-01-01", 150.0),
      Row(3, 1, "2023-01-02", 200.0)
    )

    val transactions = spark.createDataFrame(
      spark.sparkContext.parallelize(transactionData),
      transactionSchema
    )

    // Registering DataFrames as Temp Views
    customers.createOrReplaceTempView("customers")
    transactions.createOrReplaceTempView("transactions")

    // SQL Query utilizing a join that might leverage DPP
    val result = spark.sql(
      """
        |SELECT c.*, t.transaction_amount, t.transaction_date
        |FROM customers c
        |JOIN transactions t ON c.customer_id = t.customer_id
        |WHERE t.transaction_date = '2023-01-01'
        |""".stripMargin)

    result.show()

    // Stop the Spark session
    spark.stop()
  }
}
