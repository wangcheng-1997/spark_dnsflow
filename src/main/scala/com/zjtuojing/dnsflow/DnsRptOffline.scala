package com.zjtuojing.dnsflow

import java.text.SimpleDateFormat

import com.zjtuojing.dnsflow.BeanObj.{DnsBean, DnsBeanTop, EsDataBean, EsDnsAipBusiness, EsDnsAipOperator, EsDnsAipProvince, EsDnsQps, EsDnsServerTop, EsDnsUserInfo, EsRequestType, EsResponseCode, EsResponseType, authDomainMsg}
import com.zjtuojing.dnsflow.DnsRpt.call
import com.zjtuojing.utils.DNSUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object DnsRptOffline {
  private val logger: Logger = LoggerFactory.getLogger(DnsRptOffline.getClass)

  val properties = DNSUtils.loadConf()

  def main(args: Array[String]): Unit = {

    val Array(startTime, endTime) = args

    val conf = new SparkConf()
    conf.setAppName("SparkStreamingReadHDFS")
//    conf.setMaster("local[4]")
    //设置写入es参数
      .set("es.port", properties.getProperty("es2.port"))
      .set("es.nodes", properties.getProperty("es2.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es2.nodes.wan.only"))

    //采用kryo序列化库
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    conf.registerKryoClasses(
      Array(
        classOf[Array[String]],
        classOf[DnsBean],
        classOf[DnsBeanTop],
        classOf[EsDataBean],
        classOf[EsDnsUserInfo],
        classOf[EsResponseType],
        classOf[EsRequestType],
        classOf[EsResponseCode],
        classOf[EsDnsQps],
        classOf[EsDnsAipOperator],
        classOf[EsDnsAipProvince],
        classOf[EsDnsServerTop],
        classOf[EsDnsAipBusiness],
        classOf[authDomainMsg]
      )
    )
    val sparkContext = SparkContext.getOrCreate(conf)
    //HDFS HA
    sparkContext.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sparkContext.hadoopConfiguration.set("dfs.nameservices",properties.getProperty("dfs.nameservices"))
    sparkContext.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1",properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sparkContext.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    //创建SparkSQL实例
    val spark = new SQLContext(sparkContext)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    //    call(spark, sparkContext, 0)
    // 用textFileStream的API定时读取一个空的文件 实现实时框架调度离线程序
    // /dns_log/test这个文件一定要保留
    //    val ssc = new StreamingContext(sparkContext, Seconds(300))
    //    val stream = ssc.textFileStream("hdfs://nns/DontDelete")
    //    stream.foreachRDD((rdd, time) => {
    //      获取当前时间

    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (t <- startTime.toLong to endTime.toLong by 300000) {
      logger.info(s"aaaa----${t},  ${simpleDateFormat.format(t)}")
      call(spark,sparkSession,sparkContext, t)
      //    })
      //    ssc.start()
      //    ssc.awaitTermination()

    }

  }
}
