import java.text.SimpleDateFormat

object Test {
  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = format.parse("2017-11-26 09:05:00").getTime
    println(time)
  }
}
