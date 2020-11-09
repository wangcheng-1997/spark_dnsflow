import java.net.URI
import java.util.Date

import com.zjtuojing.dnsflow.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ClassName Test1
  * Date 2020/1/2 9:18
  **/
object Test1 {

  def main(args: Array[String]): Unit = {

    //由于日志里的时间戳精确到秒 所以这里把程序获取到的时间戳也截取到秒 否则会出现精度不一致
    val appTimestamp = 1577895000L
    //日志起始时间
    val startTime = appTimestamp - 60 * 16
    val endTime = appTimestamp - 60 * 5

    val hdfsLogTimestamp = appTimestamp * 1000 - 1000 * 60 * 10

    val now = Utils.timestamp2Date("yyyyMMddHHmm", hdfsLogTimestamp)
    val year = now.substring(0, 4) //年
    val month = now.substring(4, 6) //月
    val day = now.substring(6, 8) //日


    val configuration = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://30.250.60.7"), configuration)

    val pathList = fs.listStatus(new Path(s"/dns_log/${year}/${month}/${day}"))
      .map(hdfs => {
        val path = hdfs.getPath
        val logTimestamp = path.toString.split("_")(2).toLong
        (path.toString, logTimestamp)
      }).filter(tsp => {
      if (tsp._2 >= startTime && tsp._2 < endTime) {
        true
      } else {
        false
      }
    }).toList

    var dnsPaths = pathList.map(_._1).mkString(",")
    //时段0005
    val enddate = Utils.timestamp2Date("HHmm", endTime * 1000)
    //把前一天的数据拼接上
    val endday = Utils.timestamp2Date("yyyyMMdd", hdfsLogTimestamp - 1000 * 60 * 60)

    println(endday.substring(0, 4) + "|" + endday.substring(4, 6) + "|" + endday.substring(6, 8))

    if (enddate.equals("0005")) {
      dnsPaths = dnsPaths + s",hdfs://30.250.60.7/dns_log/${endday.substring(0, 4)}/${endday.substring(4, 6)}/${endday.substring(6, 8)}/2359_*"
    }

    val conf = new SparkConf().setMaster("local[4]").setAppName("app")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(dnsPaths).foreach(println(_))

  }

}
