package structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.functions._

object StreamingJoins {
  val spark = SparkSession.builder()
    .appName("Joins with streaming data")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  //read the static data
  val guitardPlayers = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")
  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")
  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  /* join with static DFs */
  val joinCondition = guitardPlayers.col("band") === bands.col("id")
  val guitarPlayersJoinBands = guitardPlayers.join(bands, joinCondition, "inner")
  val bandsSchema = bands.schema
  /* join stream Df with static Df*/
  def readFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 54321)
    .load()

  def joinStreamWithStatic() = {
    val streamedBands = readFromSocket()
      .select(from_json(col("value"),bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    //join happens per batch
    val streamedBandsGuitarPlayers = streamedBands
      .join(guitardPlayers, streamedBands.col("id") === guitardPlayers.col("band"), "inner")

    streamedBandsGuitarPlayers.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /*join stream Df with stream Df*/
  def joinStreamWithStream () = {
    val streamedBands = readFromSocket()
      .select(from_json(col("value"),bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")
    val streamedGuitarPlayers = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), guitardPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id",
        "guitarPlayer.name as name",
        "guitarPlayer.guitars as guyitars",
      "guitarPlayer.band as band")

    val streamedBandsStreamedGuitarPlayers = streamedBands
      .join(streamedGuitarPlayers,
        streamedBands.col("id") === streamedGuitarPlayers.col("band"),
      "inner")

    streamedBandsStreamedGuitarPlayers.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // joinStreamWithStatic()
    joinStreamWithStream()
  }

}
