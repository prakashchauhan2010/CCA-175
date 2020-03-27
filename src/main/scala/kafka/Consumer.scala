package kafka

import java.util.Properties
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Collection

object Consumer {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "ip-20-0-31-210.ec2.internal:9092")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeSerializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeSerializer")
    
    val topic = args(0)
    
    val c = new KafkaConsumer[String,String](prop)
    c.subscribe(util.Collections.singletonList(topic))
    
  }
}