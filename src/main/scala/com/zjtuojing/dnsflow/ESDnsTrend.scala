package com.zjtuojing.dnsflow

import java.text.SimpleDateFormat
import java.util.Date

import com.zjtuojing.dnsflow.BeanObj._
import com.zjtuojing.utils.DNSUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Response

/**
 * ClassName ESDnsFTP
 * Date 2020/6/2 17:28
 */
object ESDnsTrend {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        """
          |参数错误：
          |<taskType hh,6h>
          |""".stripMargin)
      sys.exit()
    }

    val Array(task) = args
    val properties = DNSUtils.loadConf()

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ESAggregate")
      //          .setMaster("local[*]")
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
        classOf[TrendBeanAll]
      )
    )

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    getTrendAll(spark, sc, task)

    if (task == "hh") {
      getDnsClearDD(sc)
    }

    sc.stop()
  }

  private def getDnsClearDD(sc: SparkContext): Unit = {
    // 时间戳转换日期
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val df2: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    // 获取读取数据区间
    val upper: Long = DNSUtils.getTaskTime._1._1
    val lower: Long = DNSUtils.getTaskTime._1._2

    // 获取数据读取索引
    val prefix_dnsflow_clear_index = "bigdata_dns_flow_clear_"
    val day: String = df2.format(lower * 1000)
    val yesterday: String = df2.format((lower - 86400) * 1000)

    // 5分钟数据查询语句
    val query1 = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{ \"accesstime\": { \"gte\": $lower,\"lt\": $upper}}}]}}}"""

    // 读取clear
    val sumRDD = sc.esRDD(s"$prefix_dnsflow_clear_index" + s"$day", query1)
      .values
      .map((per: collection.Map[String, AnyRef]) => {
        val accesstime: Long =
          df.parse(df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")).getTime / 1000
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
        val updateTime = System.currentTimeMillis() / 1000

        var key = clientName + authorityDomain + dnsIp + aip + domain + websiteName + creditCode + companyType + companyName + companyAddr +
          onRecord + websiteType + soft + resourceName + resourceType + resourceProps + aIpAddr

        key = s"${day}_${DNSUtils.MD5Encode(key)}"

        (key, EsDataBeanDd(key, clientName.toInt, authorityDomain, dnsIp, aip,
          domain, resolver.toLong, inNet.toLong, error.toLong, accesstime,
          websiteName, creditCode, companyType,
          companyName, companyAddr, onRecord, websiteType, soft,
          resourceName, resourceType, resourceProps,
          abroadNum.toLong, telecomNum.toLong, linkNum.toLong, gatNum.toLong, aIpAddr, updateTime))
      })

    val redis_key = s"dns:dns_clear_dd:${day}"

    val valueRDD = sumRDD.reduceByKey((a, b) => {
      val gatNum = a.gatNum + b.gatNum
      val error = a.error + b.error
      val inNet = a.inNet + b.inNet
      val resolver = a.resolver + b.resolver
      val abroadNum = a.abroadNum + b.abroadNum
      val linkNum = a.linkNum + b.linkNum
      val telecomNum = a.telecomNum + b.telecomNum

      EsDataBeanDd(a.key, a.clientName, a.authorityDomain, a.dnsIp: String, a.aip: String,
        a.domain: String, resolver: Long, inNet: Long, error: Long, a.accesstime,
        a.websiteName: String, a.creditCode: String, a.companyType: String,
        a.companyName: String, a.companyAddr: String, a.onRecord: String, a.websiteType: String, a.soft: String,
        a.resourceName: String, a.resourceType: String, a.resourceProps: String,
        abroadNum: Long, telecomNum: Long, linkNum: Long, gatNum: Long, a.aIpAddr: String, a.updateTime)
    })

    val resultRDD = valueRDD.coalesce(10).mapPartitions(per => {

      var res = new scala.collection.mutable.ListBuffer[EsDataBeanDd]()
      val jedisPool = JedisPool.getJedisPool()
      val jedis = JedisPool.getJedisClient(jedisPool)
      val pipe = jedis.pipelined()
      val updateTime = System.currentTimeMillis() / 1000

      try {
        var redisMap: Map[String, Response[String]] = Map[String, Response[String]]()
        while (per.hasNext) {
          val line = per.next()
          val accesstime = df.parse(df.format(line._2.accesstime * 1000).substring(0, 10).concat(" 00:00:00"))
          val uk = line._2.clientName + line._2.authorityDomain + line._2.dnsIp + line._2.aip + line._2.domain + line._2.websiteName +
            line._2.creditCode + line._2.companyType + line._2.companyName + line._2.companyAddr + line._2.onRecord +
            line._2.websiteType + line._2.soft + line._2.resourceName + line._2.resourceType + line._2.resourceProps + line._2.aIpAddr
          val key = s"${day}_${DNSUtils.MD5Encode(uk)}"
          val bean = EsDataBeanDd(key, line._2.clientName, line._2.authorityDomain: String, line._2.dnsIp: String, line._2.aip: String,
            line._2.domain: String, line._2.resolver: Long, line._2.inNet: Long, line._2.error: Long, accesstime.getTime / 1000,
            line._2.websiteName: String, line._2.creditCode: String, line._2.companyType: String,
            line._2.companyName: String, line._2.companyAddr: String, line._2.onRecord: String, line._2.websiteType: String,
            line._2.soft: String, line._2.resourceName: String, line._2.resourceType: String, line._2.resourceProps: String,
            line._2.abroadNum: Long, line._2.telecomNum: Long, line._2.linkNum: Long, line._2.gatNum: Long, line._2.aIpAddr, updateTime)

          redisMap += (key -> pipe.hget(redis_key, key))
          res += bean
        }
        pipe.sync()

        res.foreach(bean => {
          val value = redisMap.get(bean.key)
          if (value.get.get() != null) {
            val redisEsDataBeanDF = util.JsonTrans.Json2EsDataBeanDd(value.get.get())
            if (redisEsDataBeanDF != null) {
              if (redisEsDataBeanDF.abroadNum != null) {
                bean.abroadNum += redisEsDataBeanDF.abroadNum
              }
              if (redisEsDataBeanDF.telecomNum != null) {
                bean.telecomNum += redisEsDataBeanDF.telecomNum
              }
              if (redisEsDataBeanDF.linkNum != null) {
                bean.linkNum += redisEsDataBeanDF.linkNum
              }
              if (redisEsDataBeanDF.gatNum != null) {
                bean.gatNum += redisEsDataBeanDF.gatNum
              }
              if (redisEsDataBeanDF.resolver != null) {
                bean.resolver += redisEsDataBeanDF.resolver
              }
              if (redisEsDataBeanDF.inNet != null) {
                bean.inNet += redisEsDataBeanDF.inNet
              }
              if (redisEsDataBeanDF.error != null) {
                bean.error += redisEsDataBeanDF.error
              }
            }
          }
          pipe.hset(redis_key, bean.key, util.JsonTrans.EsDataBeanDd2Json(bean))
        })
        pipe.sync()
      } catch {
        case e: Exception => {
          logger.error("aggregateClear error", e)
        }
      } finally {
        if (pipe != null) {
          pipe.close()
        }
        if (jedis.exists(s"dns:dns_clear_dd:${yesterday}")) {
          jedis.del(s"dns:dns_clear_dd:${yesterday}")
        }
        if (jedis != null) {
          jedis.close()
        }
        if (jedisPool != null) {
          jedisPool.destroy()
        }
      }
      res.iterator
    }).persist(StorageLevel.MEMORY_AND_DISK)

    EsSpark.saveToEs(resultRDD, "bigdata_dns_flow_clear_dd/dnsflow", Map("es.mapping.id" -> "key"))

    getDomainResolver(day, sc, resultRDD)

  }

  private def getDomainResolver(day: String, sc: SparkContext, sumRDD: RDD[EsDataBeanDd]): Unit = {

    val resultRDD = sc.parallelize(sumRDD.collect)
    // 域名全量数据

    // 资源类型数据
    val allTypeDataRDD = resultRDD.map(per => ((per.domain, per.resourceType), per))
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

    val allDataRDD = resultRDD.map(per => (per.domain, per))
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
    val clientTypeDataRDD = resultRDD.map(per => ((per.domain, per.resourceType, per.clientName), per))
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

    val clientDataRDD = resultRDD.map(per => ((per.domain, per.clientName), per))
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

  def rmNull(per: collection.Map[String, AnyRef], key: String, kind: String): String = {
    var value: String = ""
    val value1 = per.getOrElse(key, "")
    if (value1 != null) value = value1.toString
    else {
      if (kind == "String") value = "null"
      else value = "0"
    }
    value
  }

  private def getTrendAll(spark: SparkSession, sc: SparkContext, task: String): Unit = {

    import spark.implicits._

    // 时间戳转换日期
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val df2: SimpleDateFormat = new SimpleDateFormat("yyyyMM")


    if ("hh".equals(task)) {
      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2
      val month = df2.format(lower * 1000)
      val year = month.substring(0,4)

      // 查询语句
      val index = "bigdata_dns_trend_" + month
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{\"accesstime\":{\"gte\":$lower,\"lt\":$upper}}}]}}}"""
      sc.esRDD(index, query)
        .values
        .map(per => {
          val domain: String = per.getOrElse("domain", "").toString
          val authorityDomain: String = per.getOrElse("authorityDomain", "").toString
          val clientName: String = per.getOrElse("clientName", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val aip: String = per.getOrElse("aip", "").toString
          val inNet: String = per.getOrElse("inNet", "").toString
          val error: String = per.getOrElse("error", "").toString
          val resolver: String = per.getOrElse("resolver", "").toString
          val websiteType: String = rmNull(per, "websiteType", "String")
          val soft: String = rmNull(per, "soft", "String")
          val websiteName: String = per.getOrElse("websiteName", "").toString
          val companyName: String = per.getOrElse("companyName", "").toString
          (domain, authorityDomain, clientName, accessTime, aip, inNet, error, resolver, websiteType, soft, websiteName, companyName)
        })
        .toDF("domain", "authorityDomain", "clientName", "accessTime", "aip", "inNet", "error", "resolver", "websiteType", "soft", "websiteName", "companyName")
        .createOrReplaceTempView("trend_avg")

      val avg_RDD: RDD[TrendBeanAll] = spark.sql(
        """
          |select
          |domain,authorityDomain,clientName,accessTime,aip,websiteType,soft,websiteName,companyName,ceil(avg(inNet)) inNet,ceil(avg(error)) error,ceil(avg(resolver)) resolver
          |from
          |trend_avg
          |group by domain,authorityDomain,clientName,accessTime,aip,websiteType,soft,websiteName,companyName
          |""".stripMargin)
        .rdd
        .map(per => {
          val domain = per.getAs[String]("domain")
          val authorityDomain = per.getAs[String]("authorityDomain")
          val clientName = per.getAs[String]("clientName").toInt
          val accesstime: Long = df.parse(per.getAs("accessTime")).getTime / 1000
          val aip = per.getAs[String]("aip")
          val inNet = per.getAs[Long]("inNet")
          val error = per.getAs[Long]("error")
          val resolver = per.getAs[Long]("resolver")
          val websiteType = per.getAs[String]("websiteType")
          val soft = per.getAs[String]("soft")
          val websiteName = per.getAs[String]("websiteName")
          val companyName = per.getAs[String]("companyName")
          TrendBeanAll(domain, authorityDomain, clientName, accesstime, aip, inNet, error, resolver, websiteType, soft, websiteName, companyName)
        })
      EsSpark.saveToEs(avg_RDD, s"bigdata_dns_trend_hh_$year/trend")

    } else if ("6h".equals(task)) {
      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._2._1
      val lower: Long = DNSUtils.getTaskTime._2._2
      val month = df2.format(lower * 1000)
      val year = month.substring(0,4)

      // 查询语句
      val index = s"bigdata_dns_trend_hh_$year"
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{\"accesstime\":{\"gte\":$lower,\"lt\":$upper}}}]}}}"""

      sc.esRDD(index, query)
        .values
        .map(per => {
          val domain: String = per.getOrElse("domain", "").toString
          val authorityDomain: String = per.getOrElse("authorityDomain", "").toString
          val clientName: String = per.getOrElse("clientName", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val aip: String = per.getOrElse("aip", "").toString
          val inNet: String = per.getOrElse("inNet", "").toString
          val error: String = per.getOrElse("error", "").toString
          val resolver: String = per.getOrElse("resolver", "").toString
          val websiteType: String = rmNull(per, "websiteType", "String")
          val soft: String = rmNull(per, "soft", "String")
          val websiteName: String = per.getOrElse("websiteName", "").toString
          val companyName: String = per.getOrElse("companyName", "").toString
          (domain, authorityDomain, clientName, accessTime, aip, inNet, error, resolver, websiteType, soft, websiteName, companyName)
        })
        .toDF("domain", "authorityDomain", "clientName", "accessTime", "aip", "inNet", "error", "resolver", "websiteType", "soft", "websiteName", "companyName")
        .createOrReplaceTempView("trend_avg")

      val avg_RDD: RDD[TrendBeanAll] = spark.sql(
        """
          |select
          |domain,authorityDomain,clientName,accessTime,aip,websiteType,soft,websiteName,companyName,ceil(avg(inNet)) inNet,ceil(avg(error)) error,ceil(avg(resolver)) resolver
          |from
          |trend_avg
          |group by domain,authorityDomain,clientName,accessTime,aip,websiteType,soft,websiteName,companyName
          |""".stripMargin)
        .rdd
        .map(per => {
          val domain = per.getAs[String]("domain")
          val authorityDomain = per.getAs[String]("authorityDomain")
          val clientName = per.getAs[String]("clientName").toInt
          val accesstime: Long = df.parse(per.getAs("accessTime")).getTime / 1000
          val aip = per.getAs[String]("aip")
          val inNet = per.getAs[Long]("inNet")
          val error = per.getAs[Long]("error")
          val resolver = per.getAs[Long]("resolver")
          val websiteType = per.getAs[String]("websiteType")
          val soft = per.getAs[String]("soft")
          val websiteName = per.getAs[String]("websiteName")
          val companyName = per.getAs[String]("companyName")
          TrendBeanAll(domain, authorityDomain, clientName, accesstime, aip, inNet, error, resolver, websiteType, soft, websiteName, companyName)
        })
      EsSpark.saveToEs(avg_RDD, "bigdata_dns_trend_6h/trend")
    } else {
      println("参数错误 ! <task:hh,6h>")
    }

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
