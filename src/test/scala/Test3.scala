import java.text.SimpleDateFormat

/**
  * ClassName Test3
  * Date 2020/1/9 10:03
  **/
object Test3 {

  def main(args: Array[String]): Unit = {

    val data = Array("hadoop", "spark", "ml", "mmmm", "fsdsdfDHCP", "DHCP5")

    data.filter(line => !line.contains("DHCP") || !line.substring(line.length - 4, line.length).equals("DHCP"))
      .foreach(println(_))

  }
}
