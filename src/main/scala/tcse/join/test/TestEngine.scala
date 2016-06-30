package tcse.join.test

/**
  * Created by DuanSky on 2016/6/27.
  */
object TestEngine {

  def main(args:Array[String]): Unit ={
    val COUNT = List("10000,10000,20","10000,10000,200","10000,10000,2000","10000,10000,5000",
      "100000,100000,20","100000,100000,200","100000,100000,2000","100000,100000,20000","100000,100000,50000")
    val DURATION = List("2,5,5","5,10,10","5,25,25")
    JoinUtil.deleteDir(JoinConfig.finalJoinResultPath)



    for(duration <- DURATION){
      var time = 1000
      for(count <- COUNT){

        println(count+"-"+duration)

        val counts = count.split(",")
        JoinConfig.aNum = counts(0).toInt
        JoinConfig.bNum = counts(1).toInt
        JoinConfig.crossNum = counts(2).toInt

        val durations = duration.split(",")
        JoinConfig.sparkStreamingDuration = durations(0).toInt
        JoinConfig.lengthTimes = durations(1).toInt
        JoinConfig.slideTimes = durations(2).toInt

        new JavaJoinProducer().produce()
        Thread.sleep(60 * time)

        SparkJoin.join()

        Thread.sleep(10 * time)

        JoinAnalysizer.analysis()

        Thread.sleep(10 * time)

        time = time * 3 / 2

      }
    }
  }

}
