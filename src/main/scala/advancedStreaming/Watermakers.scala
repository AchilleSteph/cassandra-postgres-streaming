package advancedStreaming


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp
import scala.concurrent.duration._

object Watermakers {
  val spark = SparkSession.builder()
    .appName("Streaming with watermarker")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  def sampleDataForWatermarkTest() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map {line =>
        val token  = line.split(",")
        val time_event = new Timestamp(token(0).toLong)
        val data = token(1)
        (time_event, data)
      }
      .toDF("Created", "color")

   // aggregate data using watermarkers
    val watermarkedDF = dataDF
      .withWatermark("Created", "2 seconds") //threshold = 2 seconds = watermark interval
      .groupBy(window(col("Created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    //write stream in the sink
    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) // every 2 seconds,  a new batch will be processed
      .start()
    debugQuery(query)
    query.awaitTermination()
  }

  /* Useful skill for debugging */
  def debugQuery(query : StreamingQuery) = {
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.toString()

        println(s"$i: $queryEventTime")
      }
    }).start() // start the debugging Thread
  }
  def main(args: Array[String]): Unit = {
    sampleDataForWatermarkTest()
  }

}
/* A new application to send data to the socket, throw the port number 12345*/
object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() //blocking call so that other servers could not connect on the port 12345
  val printer = new PrintStream(socket.getOutputStream)

  println("Socket accepted")

  def example1() = {
    Thread.sleep(7000)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,green")
    Thread.sleep(4000)
    printer.println("14000,blue")
    Thread.sleep(1000)
    printer.println("9000,red")
    Thread.sleep(3000)
    printer.println("150000,red")
    printer.println("80000,blue")
    Thread.sleep(1000)
    printer.println("13000,green")
    Thread.sleep(500)
    printer.println("210000,green")
    Thread.sleep(30000)
    printer.println("40000, purple")
    Thread.sleep(2000)
    printer.println("17000,green")


  }
  def main(args: Array[String]): Unit = {
    example1()
  }

}
