package com.zjtuojing.dnsflow

import java.text.SimpleDateFormat
import java.util.Date

import com.zjtuojing.dnsflow.BeanObj._
import com.zjtuojing.utils.DNSUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import scalikejdbc.{ConnectionPool, DB, SQL}
import scalikejdbc.config.DBs

/**
 * ClassName IntelliJ IDEA
 * Date 2020/6/10 11:38
 */
object ESDnsClear {

  def main(args: Array[String]): Unit = {
    val properties = DNSUtils.loadConf()

    Class.forName("com.mysql.jdbc.Driver")

    // 指定数据库连接url，userName，password

    val url = properties.getProperty("mysql.url")

    val userName = properties.getProperty("mysql.username")

    val password = properties.getProperty("mysql.password")

    ConnectionPool.singleton(url, userName, password)

    DBs.setupAll()

    val sparkConf: SparkConf = new SparkConf()
//            .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
      .set("es.port", properties.getProperty("es1.port"))
      .set("es.nodes", properties.getProperty("es1.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es1.nodes.wan.only"))
      .set("es.index.auto.create", properties.getProperty("es1.index.auto.create"))
      .set("spark.es.input.use.sliced.partitions", properties.getProperty("spark.es.input.use.sliced.partitions"))
      .set("es.index.read.missing.as.empty", "true")
    //采用kryo序列化库
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    sparkConf.registerKryoClasses(
      Array(
        classOf[Array[String]],
        classOf[EsDataBean],
        classOf[EsDataBeanDd]
      )
    )

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    // 时间戳转换日期
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val df2: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    // 获取读取数据区间
    val upper: Long = DNSUtils.getTaskTime._3._1
    val lower: Long = DNSUtils.getTaskTime._3._2

    // 获取数据读取索引
    val prefix_dnsflow_clear_index = "bigdata_dns_flow_clear_"
    val day: String = df2.format(lower * 1000)


    // 5分钟数据查询语句
    val query1 = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{ \"accesstime\": { \"gte\": $lower,\"lt\": $upper}}}]}}}"""

    import spark.implicits._

    // 读取clear
    sc.esRDD(s"$prefix_dnsflow_clear_index" + s"$day", query1)
      .values
      .map((per: collection.Map[String, AnyRef]) => {
        val accesstime: String =
          df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
        val clientName: String = per.getOrElse("clientName", "").toString
        val authorityDomain: String = rmNullString(per, "authorityDomain", "")
        val dnsIp: String = rmNullString(per, "dnsIp", "")
        val aip: String = rmNullString(per, "aip", "")
        val domain: String = rmNullString(per, "domain", "")
        val resolver: String = per.getOrElse("resolver", "").toString
        val inNet: String = per.getOrElse("inNet", "").toString
        val error: String = per.getOrElse("error", "").toString
        val websiteName: String = rmNullString(per, "websiteName", "")
        val creditCode: String = rmNullString(per, "creditCode", "")
        val companyType: String = rmNullString(per, "companyType", "")
        val companyName: String = rmNullString(per, "companyName", "")
        val companyAddr: String = rmNullString(per, "companyAddr", "")
        val onRecord: String = rmNullString(per, "onRecord", "")
        val websiteType: String = rmNullString(per, "websiteType", "")
        val soft: String = rmNullString(per, "soft", "")
        val resourceName: String = rmNullString(per, "resourceName", "")
        val resourceType: String = rmNullString(per, "resourceType", "")
        val resourceProps: String = rmNullString(per, "resourceProps", "")
        val abroadNum: String = per.getOrElse("abroadNum", "").toString
        val telecomNum: String = per.getOrElse("telecomNum", "").toString
        val linkNum: String = per.getOrElse("linkNum", "").toString
        val gatNum: String = per.getOrElse("gatNum", "").toString
        val aIpAddr: String = rmNullString(per, "aIpAddr", "")

        var key = clientName + authorityDomain + dnsIp + aip + domain + websiteName + creditCode + companyType + companyName + companyAddr +
          onRecord + websiteType + soft + resourceName + resourceType + resourceProps + aIpAddr

        key = df2.format(df.parse(accesstime).getTime) + "_" + DNSUtils.MD5Encode(key)

        EsDataBeanDF(key, clientName.toInt, authorityDomain, dnsIp, aip,
          domain, resolver.toLong, inNet.toLong, error.toLong, accesstime,
          websiteName, creditCode, companyType,
          companyName, companyAddr, onRecord, websiteType, soft,
          resourceName, resourceType, resourceProps,
          abroadNum.toLong, telecomNum.toLong, linkNum.toLong, gatNum.toLong, aIpAddr)
      })
      .toDF()
      .createOrReplaceTempView("clear_sum")

    // 聚合当天当前累计数据
    val sumRDD: RDD[EsDataBeanDd] =
      spark.sql(
        """
          |select
          |key,
          |clientName,authorityDomain,dnsIp,aip,domain,
          |accesstime,websiteName,creditCode,companyType,companyName,
          |companyAddr,onRecord,websiteType,soft,resourceName,
          |resourceType,resourceProps,aIpAddr,
          |sum(abroadNum) abroadNum,
          |sum(telecomNum) telecomNum,
          |sum(linkNum) linkNum,
          |sum(gatNum) gatNum,
          |sum(resolver) resolver,
          |sum(inNet) inNet,
          |sum(error) error
          |from clear_sum
          |group by key,
          |clientName,authorityDomain,dnsIp,aip,domain,
          |accesstime,websiteName,creditCode,companyType,companyName,
          |companyAddr,onRecord,websiteType,soft,resourceName,
          |resourceType,resourceProps,aIpAddr
          |""".stripMargin)
        .rdd
        .map((per: Row) => {
          val key: String = per.getAs("key")
          val clientName: Int = per.getAs("clientName")
          val authorityDomain: String = per.getAs("authorityDomain")
          val dnsIp: String = per.getAs("dnsIp")
          val aip: String = per.getAs("aip")
          val domain: String = per.getAs("domain")
          val accesstime: Long = df.parse(per.getAs("accesstime")).getTime / 1000
          val websiteName: String = per.getAs("websiteName")
          val creditCode: String = per.getAs("creditCode")
          val companyType: String = per.getAs("companyType")
          val companyName: String = per.getAs("companyName")
          val companyAddr: String = per.getAs("companyAddr")
          val onRecord: String = per.getAs("onRecord")
          val websiteType: String = per.getAs("websiteType")
          val soft: String = per.getAs("soft")
          val resourceName: String = per.getAs("resourceName")
          val resourceType: String = per.getAs("resourceType")
          val resourceProps: String = per.getAs("resourceProps")
          val aIpAddr: String = per.getAs("aIpAddr")
          val abroadNum: Long = per.getAs("abroadNum")
          val telecomNum: Long = per.getAs("telecomNum")
          val linkNum: Long = per.getAs("linkNum")
          val gatNum: Long = per.getAs("gatNum")
          val resolver: Long = per.getAs("resolver")
          val inNet: Long = per.getAs("inNet")
          val error: Long = per.getAs("error")
          val updateTime = System.currentTimeMillis() / 1000

          EsDataBeanDd(key, clientName, authorityDomain: String, dnsIp: String, aip: String,
            domain: String, resolver: Long, inNet: Long, error: Long, accesstime: Long,
            websiteName: String, creditCode: String, companyType: String,
            companyName: String, companyAddr: String, onRecord: String, websiteType: String, soft: String,
            resourceName: String, resourceType: String, resourceProps: String,
            abroadNum: Long, telecomNum: Long, linkNum: Long, gatNum: Long, aIpAddr: String, updateTime)
        })

    //        sumRDD.foreach(println)

        EsSpark.saveToEs(sumRDD, "bigdata_dns_flow_clear_dd/dnsflow", Map("es.mapping.id" -> "key"))

    val innetDomains: Array[(String, (String, Long))] = sumRDD.filter(_.inNet > 0)
      .map(per => {
        (per.domain,(per.aip,per.updateTime))
      })
      .reduceByKey((a, b) => {
        (a._1,a._2)
      })
      .collect()

    DB.localTx { implicit session =>
      //存储偏移量
      for (o: (String, (String, Long)) <- innetDomains) {
        SQL("insert into import_deviation (domain,aip,counts,accesstime) values (?,?,?,?) on duplicate key update counts = counts")
          .bind(o._1, o._2._1, 1, o._2._2)
          .update()
          .apply()
      }
    }

        DBs.closeAll()

    // 域名全量数据

    // 资源类型数据
    val allTypeDataRDD = sumRDD.map(per => ((per.domain, per.resourceType), per))
      .reduceByKey((a, b) => {
        a.gatNum += b.gatNum
        a.error += b.error
        a.inNet += b.inNet
        a.resolver += b.resolver
        a.abroadNum += b.abroadNum
        a.linkNum += b.linkNum
        a.telecomNum += b.telecomNum
        var dnsIp = a.dnsIp
        var aip = a.aip
        var key = a.key

        if (a.resolver < b.resolver) {
          dnsIp = b.dnsIp
          aip = b.aip
          key = b.key
        }

        EsDataBeanDd(key: String, 0, a.authorityDomain, dnsIp, aip, a.domain, a.resolver, a.inNet, a.error,
          a.accesstime, a.websiteName, a.creditCode, a.companyType, a.companyName, a.companyAddr, a.onRecord, a.websiteType,
          a.soft, a.resourceName, a.resourceType, a.resourceProps, a.abroadNum, a.telecomNum, a.linkNum, a.gatNum, a.aIpAddr,
          a.updateTime)
      })
      .map(per => {
        val a = per._2
        (a.domain, (a.resolver, a.inNet, a.error, a.resourceType, a.resourceName, a.resourceProps))
      })

    val allDataRDD = sumRDD.map(per => (per.domain, per))
      .reduceByKey((a, b) => {
        a.gatNum += b.gatNum
        a.error += b.error
        a.inNet += b.inNet
        a.resolver += b.resolver
        a.abroadNum += b.abroadNum
        a.linkNum += b.linkNum
        a.telecomNum += b.telecomNum
        var dnsIp = a.dnsIp
        var aip = a.aip
        var key = a.key

        if (a.resolver < b.resolver) {
          dnsIp = b.dnsIp
          aip = b.aip
          key = b.key
        }

        EsDataBeanDd(key: String, 0, a.authorityDomain, dnsIp, aip, a.domain, a.resolver, a.inNet, a.error,
          a.accesstime, a.websiteName, a.creditCode, a.companyType, a.companyName, a.companyAddr, a.onRecord, a.websiteType,
          a.soft, a.resourceName, a.resourceType, a.resourceProps, a.abroadNum, a.telecomNum, a.linkNum, a.gatNum, a.aIpAddr,
          a.updateTime)
      })
      .map(per => {
        val a = per._2
        var newKey = a.clientName + a.authorityDomain + a.domain + a.websiteName + a.creditCode + a.companyType + a.companyName + a.companyAddr +
          a.onRecord + a.websiteType + a.soft
        newKey = s"${day}_${DNSUtils.MD5Encode(newKey)}"
        (a.domain, EsDataClearBean(newKey, a.clientName, a.authorityDomain, a.aip, a.dnsIp, a.domain, a.resolver, a.inNet, a.error, a.accesstime, a.abroadNum, a.telecomNum,
          a.linkNum, a.gatNum, a.updateTime, a.key))
      })


    val allValueRDD: RDD[EsTypeDataClearBean] = allDataRDD.join(allTypeDataRDD)
      .map(per => {
        (per._2._1,
          Map(
            "resolver" -> per._2._2._1,
            "inNet" -> per._2._2._2,
            "error" -> per._2._2._3,
            "resourceType" -> per._2._2._4,
            "resourceName" -> per._2._2._5,
            "resourceProps" -> per._2._2._6
          ))
      })
      .groupByKey()
      .map(per => {
        EsTypeDataClearBean(per._1.key, per._1.clientName, per._1.authorityDomain, per._1.aip, per._1.dnsIp, per._1.domain, per._1.resolver, per._1.inNet,
          per._1.error, per._1.accesstime, per._1.abroadNum, per._1.telecomNum, per._1.linkNum, per._1.gatNum, per._1.updateTime,
          per._1.oldKey, per._2.toList)
      })

    EsSpark.saveToEs(allValueRDD, "bigdata_dns_clear_domain/domain", Map("es.mapping.id" -> "key"))

    //域名客户分组数据

    // 资源类型数据
    val clientTypeDataRDD = sumRDD.map(per => ((per.domain, per.resourceType, per.clientName), per))
      .reduceByKey((a, b) => {
        a.gatNum += b.gatNum
        a.error += b.error
        a.inNet += b.inNet
        a.resolver += b.resolver
        a.abroadNum += b.abroadNum
        a.linkNum += b.linkNum
        a.telecomNum += b.telecomNum
        var dnsIp = a.dnsIp
        var aip = a.aip
        var key = a.key

        if (a.resolver < b.resolver) {
          dnsIp = b.dnsIp
          aip = b.aip
          key = b.key
        }

        EsDataBeanDd(key: String, a.clientName, a.authorityDomain, dnsIp, aip, a.domain, a.resolver, a.inNet, a.error,
          a.accesstime, a.websiteName, a.creditCode, a.companyType, a.companyName, a.companyAddr, a.onRecord, a.websiteType,
          a.soft, a.resourceName, a.resourceType, a.resourceProps, a.abroadNum, a.telecomNum, a.linkNum, a.gatNum, a.aIpAddr,
          a.updateTime)
      })
      .map(per => {
        val a = per._2
        ((a.domain, a.clientName), (a.resolver, a.inNet, a.error, a.resourceType, a.resourceName, a.resourceProps))
      })

    val clientDataRDD = sumRDD.map(per => ((per.domain, per.clientName), per))
      .reduceByKey((a, b) => {
        a.gatNum += b.gatNum
        a.error += b.error
        a.inNet += b.inNet
        a.resolver += b.resolver
        a.abroadNum += b.abroadNum
        a.linkNum += b.linkNum
        a.telecomNum += b.telecomNum
        var dnsIp = a.dnsIp
        var aip = a.aip
        var key = a.key

        if (a.resolver < b.resolver) {
          dnsIp = b.dnsIp
          aip = b.aip
          key = b.key
        }

        EsDataBeanDd(key: String, a.clientName, a.authorityDomain, dnsIp, aip, a.domain, a.resolver, a.inNet, a.error,
          a.accesstime, a.websiteName, a.creditCode, a.companyType, a.companyName, a.companyAddr, a.onRecord, a.websiteType,
          a.soft, a.resourceName, a.resourceType, a.resourceProps, a.abroadNum, a.telecomNum, a.linkNum, a.gatNum, a.aIpAddr,
          a.updateTime)
      })
      .map(per => {
        val a = per._2
        var newKey = a.clientName + a.authorityDomain + a.domain + a.websiteName + a.creditCode + a.companyType + a.companyName + a.companyAddr +
          a.onRecord + a.websiteType + a.soft
        newKey = s"${day}_${DNSUtils.MD5Encode(newKey)}"
        ((a.domain, a.clientName), EsDataClearBean(newKey, a.clientName, a.authorityDomain, a.aip, a.dnsIp, a.domain, a.resolver, a.inNet, a.error, a.accesstime, a.abroadNum, a.telecomNum,
          a.linkNum, a.gatNum, a.updateTime, a.key))
      })


    val clientValueRDD: RDD[EsTypeDataClearBean] = clientDataRDD.join(clientTypeDataRDD)
      .map(per => {
        (per._2._1,
          Map(
            "resolver" -> per._2._2._1,
            "inNet" -> per._2._2._2,
            "error" -> per._2._2._3,
            "resourceType" -> per._2._2._4,
            "resourceName" -> per._2._2._5,
            "resourceProps" -> per._2._2._6
          ))
      })
      .groupByKey()
      .map(per => {
        EsTypeDataClearBean(per._1.key, per._1.clientName, per._1.authorityDomain, per._1.aip, per._1.dnsIp, per._1.domain, per._1.resolver, per._1.inNet,
          per._1.error, per._1.accesstime, per._1.abroadNum, per._1.telecomNum, per._1.linkNum, per._1.gatNum, per._1.updateTime,
          per._1.oldKey, per._2.toList)
      })

    EsSpark.saveToEs(clientValueRDD, "bigdata_dns_clear_domain/domain", Map("es.mapping.id" -> "key"))

  }

  def rmNullString(per: collection.Map[String, AnyRef], key: String, default: String): String = {
    var value: String = ""
    val value1 = per.getOrElse(key, default)
    if (value1 != null) {
      if (value1 != None) value = value1.toString
    } else {
      value = "null"
    }
    value
  }

}
