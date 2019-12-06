package project

import java.util.Properties

import org.apache.kafka.clients.producer._

import scalaj.http.{Http, HttpRequest}
object Producer1 {
  def main(args: Array[String]): Unit = {
    writeToKafka("Ford")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    // Make API call in every 5 minutes
    while(true) {
      val request: HttpRequest = Http("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=F&interval=5min&apikey=U4HV0SUO7S0J40TC")
      val record = new ProducerRecord[String, String](topic, request.asString.body)
      producer.send(record)
      Thread.sleep(300000)
      //      producer.close()
    }
  }
}
