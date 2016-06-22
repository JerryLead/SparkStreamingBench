package spark.streaming.examples.util

/**
  * Created by lijie on 16-6-15.
  */
object IntParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}
