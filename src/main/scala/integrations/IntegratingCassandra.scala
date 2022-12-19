package integrations

import com.datastax.spark.connector.cql.CassandraConnector
import common.{Car, carsSchema}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}

object IntegratingCassandra {
  val spark = SparkSession.builder()
    .appName("Integrating  streaming data into a distributed Cassandra Store")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  /**
   * Function to integrate data using Cassandra spark Connector
   */
  def writeToCassandraForeachBatches() = {
    //create a streaming dataset
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreachBatch {(batch: Dataset[Car], batchId: Long) =>
        batch.select("Name", "Horsepower")
          .write
          .cassandraFormat("cars", "public")
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  /**
   * Function to load data in Cassandra Store using ForeachWriter class
   */

    // create a ForeachWriter class based of our data object Car
    class CarCassandraForeachWriter extends ForeachWriter [Car]{
    // for each batch and for every partition of a given batch, epoch = block of data
    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open connection")
      true
    }
    override def process(car: Car): Unit = {
      connector.withSessionDo {session =>
        session.execute(
          s"""
             |insert into $keyspace.$table ("Name", "Horsepower")
             |values ('${car.Name}', ${car.Horsepower})
             |""".stripMargin)
      }
    }

    override def close(errorOrNull: Throwable): Unit = {
      println("Closing the connection")
    }
  }

  def writeIntoCassandra() = {
    //create a streaming dataset
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //writeToCassandraForeachBatches()
    writeIntoCassandra()
  }

}
