package tcse.join.test

import java.io.File

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinUtil {

  def deleteDir(path:String): Unit ={
    val file:File = new File(path)
    if(file.isFile) {
      file.delete
      return
    }
    else if(file.list!=null)
      file.list.foreach(child => deleteDir(path + File.separator + child))
    file.delete
  }

}
