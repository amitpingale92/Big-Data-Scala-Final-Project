package project
import java.util
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import java.util.Properties
import spray.json.JsValue
import spray.json._
import DefaultJsonProtocol._
import scala.collection.JavaConverters._
import org.mongodb.scala.{MongoCollection, _}
import mongo.Helpers._

object Consumer1 {
  def main(args: Array[String]): Unit = {
    consumeFromKafka("Ford")
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))

    val mongoClient: MongoClient = MongoClient()

    val database: MongoDatabase = mongoClient.getDatabase("scaladb")

    val collection: MongoCollection[Document] = database.getCollection("ford");

    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        println(data.value())
        val test: Unit = parseDataAndInsert(data, collection)
        println(test.toString)
      }
    }
  }

  def parseDataAndInsert(data: ConsumerRecord[String, String], collection: MongoCollection[Document]): Unit = {
    val responseOne: String = data.value()
    val globalMap = responseOne.parseJson.convertTo[Map[String, JsValue]]

    val metaData = globalMap.get("Meta Data").get.convertTo[Map[String, JsValue]]
    val symbol: String = metaData.get("2. Symbol").get.toString().replaceAll("\"", "")
    println(symbol)

    val reviewMap = globalMap.get("Time Series (5min)").get.convertTo[Map[String, JsValue]]

    for ((k, v) <- reviewMap) {
      val timeStamp = k
      val value = v.convertTo[Map[String, JsValue]]

      val open = value.get("1. open").get.toString().replaceAll("\"", "").toDouble
      val high = value.get("2. high").get.toString().replaceAll("\"", "").toDouble
      val low = value.get("3. low").get.toString().replaceAll("\"", "").toDouble
      val close = value.get("4. close").get.toString().replaceAll("\"", "").toDouble
      val volume = value.get("5. volume").get.toString().replaceAll("\"", "").toDouble

      println(symbol + " " + timeStamp + " " + open + " " + high + " " + low + " " + close + " " + volume)

      val doc: Document = Document(
        "Symbol" -> symbol,
        "Timestamp" -> timeStamp,
        "Open" -> open,
        "High" -> high,
        "Low" -> low,
        "Close" -> close,
        "Volume" -> volume
      )
      //Push data into MongoDB
      collection.insertOne(doc).results()
    }
  }

}
