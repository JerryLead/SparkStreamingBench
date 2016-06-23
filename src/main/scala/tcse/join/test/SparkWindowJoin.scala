package tcse.join.test

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.streaming.examples.StreamingExamples

/**
  * Created by DuanSky on 2016/6/21.
  */
object SparkWindowJoin {

  def main(args:Array[String]): Unit ={

    StreamingExamples.setStreamingLogLevels()

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkWindowJoin")
    val ssc = new StreamingContext(sparkConf,Seconds(JoinConfig.sparkStreamingDuration))

    //get stream of type a and b
    val topics = JoinConfig.topics.split(",")
    val aMap = getStream(topics(0),ssc).window(JoinConfig.windowLength,JoinConfig.slideInterval)
    val bMap = getStream(topics(1),ssc).window(JoinConfig.windowLength,JoinConfig.slideInterval)

    val joinRes = aMap.join(bMap)

    //for every test we will delete the old files.
    JoinUtil.deleteDir(JoinConfig.sparkWindowJoinFilePath)

    aMap.print()
    bMap.print()
    joinRes.print()

    aMap.saveAsTextFiles(JoinConfig.sparkWindowJoinTypeAFilePath)
    bMap.saveAsTextFiles(JoinConfig.sparkWindowJoinTypeBFilePath)
    joinRes.saveAsTextFiles(JoinConfig.sparkWindowJoinResultFilePath)

    ssc.start()
    ssc.awaitTermination()

  }

  def getStream(topic:String,ssc:StreamingContext) :DStream[(String,String)]={
    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> JoinConfig.brokers)
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
  }
}
