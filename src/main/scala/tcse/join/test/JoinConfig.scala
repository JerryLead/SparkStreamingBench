package tcse.join.test

import org.apache.spark.streaming.Seconds

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinConfig {

  //the number of k-v pairs of type a,b and both.
  val aNum = 10000; val bNum = 10000; val crossNum = 1000

  //spark streaming config
  val sparkStreamingDuration = 1

  //spark window config
  val lengthTimes = 5 //for window's length must be a multiple of the slide duration.
  val slideTimes = 5 //for window's length must be a multiple of the slide duration.
  val windowLength = Seconds(sparkStreamingDuration * lengthTimes)
  val slideInterval = Seconds(sparkStreamingDuration * slideTimes)

  //file path
  val aFilePath = "E://Programming/Paper/data/a.txt"
  val bFilePath = "E://Programming/Paper/data/b.txt"
  val sparkJoinFilePath = "E://Programming/Paper/data/ab/"

  val sparkStreamingJoinFilePath = "E://Programming/Paper/data/spark_streaming/"
  val sparkStreamingJoinTypeAFilePath = sparkStreamingJoinFilePath + "a/"
  val sparkStreamingJoinTypeBFilePath = sparkStreamingJoinFilePath + "b/"
  val sparkStreamingJoinResultFilePath = sparkStreamingJoinFilePath +"ab/"

  val sparkWindowJoinFilePath = "E://Programming/Paper/data/spark_window/"
  val sparkWindowJoinTypeAFilePath = sparkWindowJoinFilePath + "a/"
  val sparkWindowJoinTypeBFilePath = sparkWindowJoinFilePath + "b/"
  val sparkWindowJoinResultFilePath = sparkWindowJoinFilePath + "ab/"

  //kafka config
  val brokers = "133.133.134.13:9092"
  val zkQuorum = "133.133.134.13:2181"
//  val brokers = "133.133.134.175:9092"
//  val zkQuorum = "133.133.134.175:40003"
  val topics = "join-test-a-2,join-test-b-2"
  val group = "duansky-2"
  val threadNumber = 1

}
