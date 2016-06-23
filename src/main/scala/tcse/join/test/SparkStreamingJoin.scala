package tcse.join.test

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.streaming.examples.StreamingExamples

/**
  * Created by DuanSky on 2016/6/17.
  */
object SparkStreamingJoin {

  def main(args:Array[String]): Unit ={

    StreamingExamples.setStreamingLogLevels()

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingJoin")

    val ssc = new StreamingContext(sparkConf,Seconds(JoinConfig.sparkStreamingDuration))

    //get stream of type a and b
    val topics = JoinConfig.topics.split(",")
    val aMap = getStream(topics(0),ssc)
    val bMap = getStream(topics(1),ssc)

    val joinRes = aMap.join(bMap)

    //for every test we will delete the old files.
    JoinUtil.deleteDir(JoinConfig.sparkStreamingJoinFilePath)

    aMap.print()
    bMap.print()
    joinRes.print()

    aMap.saveAsTextFiles(JoinConfig.sparkStreamingJoinTypeAFilePath)
    bMap.saveAsTextFiles(JoinConfig.sparkStreamingJoinTypeBFilePath)
    joinRes.saveAsTextFiles(JoinConfig.sparkStreamingJoinResultFilePath)

    ssc.start()
    ssc.awaitTermination()

  }

  // pay attention: if the group is the same as the group of the spark window,
  // one of them will not receive data.
  // Pay attention: here we use createDirectStream instead of createStream.
  def getStream(topic:String,ssc:StreamingContext) :DStream[(String,String)]={
    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> JoinConfig.brokers)
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
  }

//  def getStream(topic:String,ssc:StreamingContext) :DStream[(String,String)]={
//    val topicMap = Map(topic->JoinConfig.threadNumber)
//    KafkaUtils.createStream(ssc, JoinConfig.zkQuorum, JoinConfig.group+"-streaming", topicMap)
//  }

}
