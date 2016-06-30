package tcse.join.test

import org.apache.spark.streaming.Seconds

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinConfig {

  //the number of k-v pairs of type a,b and both.
  var aNum = 10000; var bNum = 10000; var crossNum = 20

  //spark streaming config
  var sparkStreamingDuration = 5

  //spark streaming window config
  var lengthTimes = 5 //for window's length must be a multiple of the slide duration.
  var slideTimes = 5 //for window's length must be a multiple of the slide duration.
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

  val finalJoinResultPath = "E://Programming/Paper/data/result.txt"

  //kafka config
  //caizheng computer
//  val brokers = "133.133.134.13:9092"
//  val zkQuorum = "133.133.134.13:2181"
  //yingying computer
//  val brokers = "133.133.134.175:9092"
//  val zkQuorum = "133.133.134.175:40003"
  //my own computer
  val brokers = "133.133.61.117:9092"
  val zkQuorum = "133.133.61.117:2182"

  val topics = "a,b"
  val group = "duansky-5"
  val threadNumber = 1

}
