package kafka

import org.apache.kafka.clients.producer._
import java.util.Properties

object Producer {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val p = new KafkaProducer[String, String](prop)
    val topic = "demo"

    for (i <- 1 to 50) {
      val record = new ProducerRecord(topic, "key"+i, s"hello $i")
      p.send(record)
    }
  }
}