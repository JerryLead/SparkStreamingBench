package tcse.kafka.test

import java.util
import java.util.Date

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import tcse.join.test.JoinConfig

/**
  * Created by DuanSky on 2016/6/24.
  */
object ScalaProducer {

  def main(args:Array[String]): Unit ={
    val producer = getProducer()

    (1 to Config.count).foreach(key => {
      Config.topics.split(",").foreach( topic =>{
        val data = topic+new Date().toString
        val message = new ProducerRecord[String,String](topic, key+"", data)
        producer.send(message)
        System.out.println(topic + "#" + key + "#" + data)
        Thread.sleep(100)
      })
    })

  }

  def getProducer(): KafkaProducer[String, String] ={
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG,"-1")

    new KafkaProducer[String, String](props)
  }
}
