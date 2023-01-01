package structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingAggregations {
  val spark  = SparkSession.builder()
    .appName("Aggregations with Streaming data")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def linesCount() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 54321)
      .load()
    val lineCount = lines.selectExpr("count(*) as NumberOfLines")
    //write to the console
    lineCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
  def numericalAggregation() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 54321)
      .load()
    val numerik = lines.select(col("value")
      .cast("integer").as("Number"))
    val sumDF = numerik.select(sum(col("number"))).as("SumOfInputNumbers")

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Can also use a lambda function to make that method more flexible like belaw
   * def numericalAggregation(aggrFunction: Column => Column).
   * in order to give the opportunity to choose the aggregation function to all
   * Example: numericalAggregation(mean)
   */

    def readStreamData() = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 54321)
      .load()

  /**
   * Function to count names occurrences
   */
  def groupNames():Unit = {
    val names = readStreamData()
    val countNames = names.select(col("value").as("Names"))
      .groupBy(col("Names"))
      .count()

    countNames.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //linesCount()
    //numericalAggregation()
    groupNames()
  }

}
