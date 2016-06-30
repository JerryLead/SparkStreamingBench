package tcse.join.test

import java.io.File
import java.util.Properties

import kafka.consumer.ConsumerConfig
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import spark.streaming.examples.StreamingExamples

/**
  * Created by DuanSky on 2016/6/22.
  */
object KafkaTest {

  def main(args:Array[String]): Unit ={
    StreamingExamples.setStreamingLogLevels()
//    consumer
//    consumer0
    tracker
  }


  def consumer0(): Unit ={
    val props:Properties = new Properties()
    props.put("zookeeper.connect",JoinConfig.zkQuorum)
    props.put("group.id",JoinConfig.group)
    props.put("zookeeper.session.timeout.ms","6000")
    val kafkaConf = new ConsumerConfig(props)
    val topicMap = JoinConfig.topics.split(",").map(x => (x,1)).toMap
    val consumer = kafka.consumer.Consumer.create(kafkaConf)
    val streamMap = consumer.createMessageStreams[String,String](topicMap, new StringDecoder(),
      new StringDecoder())

    var count = 0

    topicMap.foreach( x =>{
      streamMap.get(x._1).foreach(f = list => list.foreach { stream =>
        val iter = stream.iterator()
        while (iter.hasNext()) {
          count += 1
          val message = iter.next()
          println(count + "#" + message.topic + message.key() + message.message())
        }
      })
    })
  }

  def consumer(): Unit ={
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingJoin")
    val ssc = new StreamingContext(sparkConf,Seconds(JoinConfig.sparkStreamingDuration))

    // Create direct kafka stream with brokers and topics
    val topicsSet = JoinConfig.topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> JoinConfig.brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.print()
    JoinUtil.deleteDir("E://Programming/Paper/data/kafka-test/")
    messages.saveAsTextFiles("E://Programming/Paper/data/kafka-test/")

    ssc.start()
    ssc.awaitTermination()
  }

  def tracker(): Unit ={
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("JoinAnalysizer")
    val sparkCont = new SparkContext(sparkConf)

    val sj = sparkCont.textFile("E://Programming/Paper/data/kafka-test" + File.separator + "*" + File.separator + "part-*")
    println("kafka reveiver data count:" + sj.count())
  }
}
