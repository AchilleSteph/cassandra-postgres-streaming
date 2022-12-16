package integrations

import common.carsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IntegratingKafka {
  val spark = SparkSession.builder()
    .appName("Kafka Integration")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  /**
   * Function to read data from the kafka-producer and print them on the console
   */

  def readFromKafka() = {
    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "training")
      .load()

    // print it in the console
    kafkaDf.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
   * A function to read data from kafka-producer and make a key-value DAtaframe,
   * readable by kafka
   */

  def readFromKafkaAlt() = {
    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "training")
      .load()

    // print it in the console
    kafkaDf.select(col("topic"), expr("CAST(value as STRING) as newValue")  )
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
   * Function to read data in json format and write them into kafka in string format.
   * Use a kafka-consumer to check if the was successfully done.
   */

  def writeIntoKafka() = {
    // make a DataFrame
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    //make a key-value DataFrame to be properly written into kafka
    val carsKafkaDF = carsDF.selectExpr("UPPER(Name) as key", "Name as value")

    // write into kafka
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "carsHistory")
      .option("checkpointLocation", "checkpoints") //without checkpoints, writeStream will fail
      .start()
      .awaitTermination()

  }

  /**
   * Function to read data from a json format and ingest them into kafka, in jsan format.
   * Use a kafka-consumer to check if the was successfully done.
   */

  def writeIntoKafkaInJson() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsJsonKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Cylinders"), col("Horsepower")))
        .cast("String").as("value")
    )

    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "carsHistory")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // readFromKafkaAlt()
    // writeIntoKafka()
    writeIntoKafkaInJson()
  }

}
