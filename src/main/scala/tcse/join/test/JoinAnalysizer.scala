package tcse.join.test

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import spark.streaming.examples.StreamingExamples

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinAnalysizer {

  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("JoinAnalysizer")
  val sparkCont = new SparkContext(sparkConf)

  def main(args:Array[String]): Unit ={
    StreamingExamples.setStreamingLogLevels()

    analysis
  }



  //analysis spark join and spark streaming join
  def analysis(): Unit ={

    //spark join result
    val sj = sparkCont.textFile(JoinConfig.sparkJoinFilePath + File.separator + "part-*")

    //spark streaming receives type a and b
    val ssjA = sparkCont.textFile(JoinConfig.sparkStreamingJoinTypeAFilePath + File.separator + "*" + File.separator  + "part-*")
    val ssjB = sparkCont.textFile(JoinConfig.sparkStreamingJoinTypeBFilePath + File.separator + "*" + File.separator  + "part-*")
    //spark streaming result
    val ssj = sparkCont.textFile(JoinConfig.sparkStreamingJoinResultFilePath + File.separator + "*" + File.separator + "part-*")
//    printf("spark streaming join:(%d,%d)=>%d\n",ssjA.count(),ssjB.count(),ssj.count())

    //spark streaming window based receives type a and b
    val sswjA = sparkCont.textFile(JoinConfig.sparkWindowJoinTypeAFilePath + File.separator + "*" + File.separator  + "part-*")
    val sswjB = sparkCont.textFile(JoinConfig.sparkWindowJoinTypeBFilePath + File.separator + "*" + File.separator  + "part-*")
    //spark streaming result
    val sswj = sparkCont.textFile(JoinConfig.sparkWindowJoinResultFilePath + File.separator + "*" + File.separator + "part-*")
//    printf("spark streaming window based join:(%d,%d)=>%d\n",sswjA.count(),sswjB.count(),sswj.count())

    printf("spark join | spark streaming join | spark streaming window based join\n" +
      "%d|(%d,%d)=>%d|(%d,%d)=>%d\n",sj.count(),ssjA.count(),ssjB.count(),ssj.count(),sswjA.count(),sswjB.count(),sswj.count())

    //delete the old files.
    JoinUtil.deleteDir(JoinConfig.sparkStreamingJoinFilePath)
    JoinUtil.deleteDir(JoinConfig.sparkWindowJoinFilePath)
  }
}
