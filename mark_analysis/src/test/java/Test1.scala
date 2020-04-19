import java.util.UUID

object Test1 {
  def main(args: Array[String]): Unit = {
    val string = UUID.randomUUID().toString.replace("-","").toLong

  }
}
