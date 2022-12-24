package advancedStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object EventTimeWindows {
  val spark = SparkSession.builder()
    .appName("Spark Streaming with Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val purchasesSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), purchasesSchema).as("purchase"))
    .selectExpr("purchase.*")

  /**
   * Function to show the total quantity sold of each product, for every hour
   */

  def analyticsPurchasesSlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    //create the window
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("TotalQuantity"))
      .select(
        col("time").getField("start").as("Start"),
        col("time").getField("end").as("End"),
        col("totalQuantity")
      )
      .sort("Start")
    // "time" in the as("time") is a struct column: time = ("start', "end")

    // Last step: write te stream in the sink
    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Function to show the total quantity sold of each product, for every day
   */

  def analyticsPurchasesTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    //create the tumbling window = with no slide duration
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time"))
      .agg(sum("quantity").as("TotalQuantity"))
      .select(
        col("time").getField("start").as("StartingDay"),
        col("time").getField("end").as("NextDay"),
        col("totalQuantity")
      )
      .sort("Start")
    // "time" in the as("time") is a struct column: time = ("start', "end")

    // Last step: write te stream in the sink
    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Function to show the best selling product of every day, + quantity sold
   * @return
   */

  //reading stream directly from json file
  def readPurchasesFromFile() = spark.readStream
    .schema(purchasesSchema)
    .json("src/main/resources/data/purchases")

  def bestSellingPerDay() = {
    val purchasesFromFileDF = readPurchasesFromFile()
    //create the window
    val windowByDay = purchasesFromFileDF
      .groupBy(col("item"), window(col("time"), "1 day").as("Day"))
      .agg(sum("quantity").as("TotalQuantity"))
      .select(
        col("Day").getField("start").as("StartingDay"),
        col("Day").getField("end").as("EndingDay"),
        col("item").as("Products"),
        col("totalQuantity")
      )
      .orderBy(col("Day"), col("totalQuantity").desc)
    // "time" in the as("time") is a struct column: time = ("start', "end")

    // Last step: write te stream in the sink
    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Function to show best selling product of every 24 hours, updated hour after hour
   * @param args
   */

    def bestSellingProductPer24h() = {
      val purchasesFromFileDF = readPurchasesFromFile()
      //create the window
      val windowByDay = purchasesFromFileDF
        .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
        .agg(sum("quantity").as("TotalQuantity"))
        .select(
          col("time").getField("start").as("Start"),
          col("time").getField("end").as("End"),
          col("item").as("Products"),
          col("totalQuantity")
        )
        .orderBy(col("start"), col("totalQuantity").desc)
      // "time" in the as("time") is a struct column: time = ("start', "end")

      // Last step: write te stream in the sink
      windowByDay.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()
    }


  def main(args: Array[String]): Unit = {
    // analyticsPurchasesSlidingWindow()
    // analyticsPurchasesTumblingWindow()
    // bestSellingPerDay()
    bestSellingProductPer24h()

  }

}
