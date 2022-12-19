package integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

object IntegratingKafkaDStreams {
  val spark = SparkSession.builder()
    .appName("Integrating Low_level DStreams with kafka")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.rest" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "bigdata"

  /**
   * Function to read data from a kafka topic named "bigdata"
   */

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group_bigdata"))
    )
    val processedDstream = kafkaDStream.map(
      record => (record.key(), record.value())
      )
    processedDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Function to write data into a kafka topic
   */
    def writeIntoKafka() = {
      val inputdata = ssc.socketTextStream("localhost", 12345)

      //transformation
      val processedData = inputdata.map(_.toUpperCase())

      processedData.foreachRDD{rdd =>
        rdd.foreachPartition{partition =>

          val kafkaHashMap = new util.HashMap[String, Object]()
          kafkaParams.foreach(pair => kafkaHashMap.put(pair._1, pair._2))

          val producer = new KafkaProducer[String, String](kafkaHashMap)
          partition.foreach { record =>
            val message = new ProducerRecord[String, String](kafkaTopic, null, record)
            //key = null and value = record
            producer.send(message)
          }
          producer.close()
        }
      }
      ssc.start()
      ssc.awaitTermination()
    }

  def main(args: Array[String]): Unit = {
    // readFromKafka()
    writeIntoKafka()
  }

}
