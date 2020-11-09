package com.zjtuojing.dnsflow

import com.zjtuojing.dnsflow.BeanObj.{DnsBean, DnsBeanTop, EsDataBean, EsDataBeanDF, EsDataBeanDd, EsDnsAipBusiness, EsDnsAipOperator, EsDnsAipProvince, EsDnsQps, EsDnsServerTop, EsDnsUserInfo, EsRequestType, EsResponseCode, EsResponseType, authDomainMsg}
import com.zjtuojing.dnsflow.DnsRpt.{call, logger}
import com.zjtuojing.utils.DNSUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Auther Cedric_Won
 * @Date 2020-09-26 12:53 
 * @Version
 */
object ImportData {
  def main(args: Array[String]): Unit = {
    val properties = DNSUtils.loadConf()
    val conf = new SparkConf()
    conf.setAppName("SparkStreamingReadHDFS")
//          conf.setMaster("local[*]")
      //设置写入es参数
      .set("es.port", properties.getProperty("es2.port"))
      .set("es.nodes", properties.getProperty("es2.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es2.nodes.wan.only"))
      .set("es.index.read.missing.as.empty", "true")
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
        classOf[authDomainMsg],
        classOf[EsDataBean],
        classOf[EsDataBeanDd],
        classOf[EsDataBeanDF]
      )
    )
    val sparkContext = SparkContext.getOrCreate(conf)
    //HDFS HA
    sparkContext.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sparkContext.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sparkContext.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sparkContext.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    //    test(sparkContext)
    //创建SparkSQL实例
    val spark = new SQLContext(sparkContext)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //    call(spark, sparkContext, 0)
    // 用textFileStream的API定时读取一个空的文件 实现实时框架调度离线程序
    // /dns_log/test这个文件一定要保留
    val lower = args(0).toLong
    val upper = args(1).toLong

    for (date <- lower to upper by  300){
      val time = date * 1000
      call(spark, sparkSession, sparkContext, time)

    }

    //    println(time)
  }
}
