package integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object IntegratingJDBC {
  val spark = SparkSession.builder()
    .appName("Integration into postgres DB using JDBC connector")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  // Connection properties for JDBC connector
  val driver = "org.postgresql.Driver"
  val url = "jdbc.postgresql://localhost:5432/ashsteph"
  val user = "docker"
  val password = "docker"

  import spark.implicits._ // for Car encoder

  // Function to write a json data into a postgres database
  def writeStreamIntoPostgres() ={
    // create a streaming Dataset
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]
    // Use foreachBatch() to load carsDS
    carsDS.writeStream
      .foreachBatch {(batch: Dataset[Car], batchId: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamIntoPostgres()

  }


}
