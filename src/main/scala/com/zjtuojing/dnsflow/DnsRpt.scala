package com.zjtuojing.dnsflow

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ThreadLocalRandom

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zjtuojing.dnsflow.BeanObj._
import com.zjtuojing.utils.DNSUtils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, Response}
import util.{IpSearch, IpUtil}

import scala.collection.mutable.ListBuffer
import scala.util.Random


/**
 * ClassName SparkStreamingReadHDFS
 * Date 2019/12/24 16:01
 **/
object DnsRpt {

  //dns四维度聚合索引
  //val dnsRptIndex = "bigdata_dns_flow_clear/dnsflow"

  private val logger: Logger = LoggerFactory.getLogger(DnsRpt.getClass)

  val middle = ""
  //  val month = new SimpleDateFormat("yyyyMM").format(new Date().getTime)

  //QPS 解析类型/响应类型/响应代码占比
  //  val dnsRatioIndex = s"bigdata_dns_flow_ratio${middle}/ratio"
  val newDnsRatioIndex = s"bigdata_dns_flow_ratio_v1${middle}/ratio"

  //省份/运营商/业务 TopN 用户/电话/地址
  val dnsTop = s"bigdata_dns_flow_top_v2${middle}/aip"
  val dnsTopDd = s"bigdata_dns_flow_top_dd${middle}/aip"

  val trendPrefix = s"bigdata_dns_trend${middle}_"

  val bigDataDnsFlowClearPrefix = s"bigdata_dns_flow_clear${middle}_"
  //文件分割时间间隔，秒
  val step = 10

  val random = ThreadLocalRandom.current()

  //var stream: DStream[String] = _


  /**
   * 基础数据聚合后写入es
   */
  def getDetailReport(detailRDD: RDD[EsDataBean], end: Long, now: String) = {
    val reportRdd = detailRDD.map(bean => {
      ((bean.clientName, bean.domain, bean.aip, bean.companyName, bean.authorityDomain, bean.soft, bean.websiteName, bean.websiteType), (bean.resolver, bean.inNet, bean.error))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .filter(_._2._1 > 100)
      .map(line => {
        val key = end + DNSUtils.MD5Encode(line._1._2 + line._1._3 + line._1._1)
        DnsReport(line._1._1, line._1._2, line._1._3, line._1._4, line._1._5, line._1._6, line._1._7, line._1._8, line._2._1, line._2._2, line._2._3, end, key)
      })

    EsSpark.saveToEs(reportRdd, s"${trendPrefix}${now.substring(0, 6)}/trend", Map("es.mapping.id" -> "key"))
  }


  /**
   * 过滤包含Top20权威域名的所有数据
   */
  def Top20AuthorityDomain(baseRDD: RDD[DnsBean], businessIpRules: Array[(Long, Long, String, String, String)]): RDD[DnsBeanTop] = {
    val domainTop20 = baseRDD.filter(bean => {
      bean.domain.contains("qq.com") ||
        bean.domain.contains("baidu.com") ||
        bean.domain.contains("taobao.com") ||
        bean.domain.contains("p2cdn.com") ||
        bean.domain.contains("root-servers.net") ||
        bean.domain.contains("jd.com") ||
        bean.domain.contains("sina.com.cn") ||
        bean.domain.contains("microsoft.com") ||
        bean.domain.contains("360.com") ||
        bean.domain.contains("w3.org") ||
        bean.domain.contains("aliyun.com") ||
        bean.domain.contains("360.cn") ||
        bean.domain.contains("apple.com") ||
        bean.domain.contains("hicloud.com") ||
        bean.domain.contains("ys7.com") ||
        bean.domain.contains("163.com") ||
        bean.domain.contains("cuhk.edu.hk") ||
        bean.domain.contains("redhat.com") ||
        bean.domain.contains("miwifi.com") ||
        bean.domain.contains("vivo.com.cn")
    }).map(bean => {
      ((bean.clientName, bean.domain, bean.dnsIp, bean.aip), (bean.resolver, bean.error))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter(_._2._1 > 100)
      .map(tuple => {
        val longIp = Utils.ipToLong(tuple._1._4)
        //资源名称 资源类型 资源属性
        var resourceName = ""
        var resourceType = ""
        var resourceProps = ""
        businessIpRules.foreach(rule => {
          if (rule._1 <= longIp && rule._2 >= longIp) {
            resourceName = rule._3
            resourceType = rule._4
            resourceProps = rule._5
          }
        })
        DnsBeanTop(tuple._1._1, tuple._1._2, tuple._1._3, tuple._1._4, tuple._2._1, tuple._2._2, longIp, resourceName, resourceType, resourceProps)
      })
    domainTop20
  }

  /**
   * 把user domain resolver聚合后写入hdfs
   */
  def SaveUserInfo2HDFS(baseRDD: RDD[DnsBean], userMap: Map[String, String], now: String) = {

    val reduced = baseRDD.mapPartitions(iter => new Iterator[((String, String), Int)]() {
      def hasNext: Boolean = {
        iter.hasNext
      }

      def next: ((String, String), Int) = {
        val bean = iter.next()
        ((bean.clientIp, bean.domain), 1)
      }
    })
      .reduceByKey(_ + _, 20)

    //      iter.hasNext
    //
    //      ((bean.clientIp, bean.domain), 1)
    //    }).reduceByKey(_ + _)

    logger.info("reduced.partitions.size: {}", reduced.partitions.size)

    reduced.map(tuple => {
      val userName = userMap.getOrElse(tuple._1._1, "")
      (userName, tuple._1._2, tuple._2)
    }).filter(tuple => StringUtils.isNotEmpty(tuple._1))
      .map(filtered => {
        filtered._1 + "\001" + filtered._2 + "\001" + filtered._3
      })
      .coalesce(6)
      .saveAsTextFile(s"hdfs://nns/dns_middle_data/${now.substring(0, 8)}/${now}")
  }


  /**
   * 获取权威Redis权威域名详细信息
   */
  def getAuthorityDomainMessage(jedis: Jedis): Array[authDomainMsg] = {
    val array = jedis.hgetAll("dns:dns-authDomain").values().toArray()
    //解析json
    var authDomain = ""
    var websiteName = ""
    var creditCode = ""
    var companyType = ""
    var companyName = ""
    var companyAddr = ""
    var onRecord = ""
    var websiteType = ""
    var soft = ""
    var jobj = JSON.parseObject("")
    val authDomainTuple = array.map(arr => {
      try {
        jobj = JSON.parseObject(arr.toString)
        authDomain = jobj.getString("authDomain")
        websiteName = jobj.getString("websiteName")
        creditCode = jobj.getString("creditCode")
        companyType = jobj.getString("companyType")
        companyName = jobj.getString("companyName")
        companyAddr = jobj.getString("companyAddr")
        onRecord = jobj.getString("onRecord")
        websiteType = jobj.getString("websiteType")
        soft = jobj.getString("soft")
        authDomainMsg(authDomain, websiteName, creditCode, companyType, companyName, companyAddr, onRecord, websiteType, soft)
      } catch {
        case e: Exception => {
          logger.error("getAuthorityDomainMessage error", e)
          authDomainMsg(authDomain, websiteName, creditCode, companyType, companyName, companyAddr, onRecord, websiteType, soft)
        }
      }
    })
    authDomainTuple
  }


  /**
   * Top --clientName, clientIp, dnsIp, domain, aip
   * 聚合数据写入ES
   */
  def Tuple2Es(baseTopRdd: RDD[DnsBeanTop],
               inNetRule: Array[(Long, Long, String)],
               time: Long,
               businessIpRules: Array[(Long, Long, String, String, String)],
               authDomainBeans: Array[authDomainMsg],
               now: String
              ): RDD[EsDataBean] = {
    //整理数据
    val resultRDD: RDD[EsDataBean] = baseTopRdd.map(bean => {
      //解析权威域名
      val authorityDomain = Utils.domian2Authority(bean.domain)
      var replaceDomain = bean.domain
      if (bean.domain.contains("http://")) replaceDomain = bean.domain.replace("http://", "")
      if (bean.domain.contains("https://")) replaceDomain = bean.domain.replace("https://", "")

      //内网
      var inNet = 0L
      //匹配大表
      inNetRule.foreach(tuple => {
        if (tuple._1 <= bean.longIp && tuple._2 >= bean.longIp) {
          inNet = bean.resolver
        } else {
          //匹配小表
          businessIpRules.foreach(rule => {
            if (rule._1 <= bean.longIp && rule._2 >= bean.longIp) {
              inNet = bean.resolver
            }
          })
        }
      })

      //权威域名详细数据
      var websiteName = ""
      var creditCode = ""
      var companyType = "未知"
      var companyName = ""
      var companyAddr = ""
      var onRecord = ""
      var websiteType = "未知"
      var soft = ""

      authDomainBeans.foreach(bean => {
        if (authorityDomain.equals(bean.authDomain)) {
          websiteName = bean.websiteName
          creditCode = bean.creditCode
          companyType = bean.companyType
          companyName = bean.companyName
          companyAddr = bean.companyAddr
          onRecord = bean.onRecord
          websiteType = bean.websiteType
          soft = bean.soft
        }
      })

      val maps = IpSearch.getRegionByIp(bean.aip)
      //电信 联通 国家 港澳台
      var abroadNum = 0L
      var telecomNum = 0L
      var linkNum = 0L
      var gatNum = 0L
      var aIpAddr = ""
      if (!maps.isEmpty) {
        //获取国家，运营商
        val country = maps.get("国家").toString
        val operate = maps.get("运营").toString
        if (!country.equals("中国")) abroadNum = bean.resolver
        if (operate.contains("电信")) telecomNum = bean.resolver
        if (operate.contains("联通")) linkNum = bean.resolver
        //获取省份
        val province = maps.get("省份").toString
        if (province.contains("香港") || province.contains("澳门") || province.contains("台湾")) {
          gatNum = bean.resolver
        }
        //获取城市
        val city = maps.get("城市").toString
        var provinceAndCity = province ++ city
        if (city.equals(province)) provinceAndCity = province
        aIpAddr = country ++ provinceAndCity ++ operate
      }


      EsDataBean(bean.clientName, authorityDomain, bean.dnsIp, bean.aip, replaceDomain, bean.resolver, inNet, bean.error, time,
        websiteName, creditCode, companyType, companyName, companyAddr, onRecord, websiteType, soft, bean.resourceName, bean.resourceType,
        bean.resourceProps, abroadNum, telecomNum, linkNum, gatNum, aIpAddr)
    })


    EsSpark.saveToEs(resultRDD, s"${bigDataDnsFlowClearPrefix}${now.substring(0, 8)}/dnsflow")

    resultRDD
  }


  /**
   * 读取Redis key-cache-userinfo 获取电话号码 地址
   */
  def getPhoneNumAndAddress(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      val redisArray = jedis.hgetAll("broadband:userinfo").values().toArray()
      maps = redisArray.map(array => {
        val jobj = JSON.parseObject(array.toString)
        var mobile = "未知"
        if (jobj.getString("mobile") != "") {
          mobile = jobj.getString("mobile")
        }
        (jobj.getString("username"), jobj.getString("doorDesc") + "\t" + mobile)
      }).toMap
    } catch {
      case e: Exception => {
        maps = maps
        logger.error("getPhoneNumAndAddress", e)
        //        e.printStackTrace()
      }
    }
    maps
  }


  /**
   * 读取Redis key-cache-online-user 获取用户名
   */
  def getUserName(jedis: Jedis): Map[String, String] = {
    var maps = Map[String, String]()
    try {
      maps = jedis.hgetAll(jedis.hget("ONLINEUSERS:CONSTANTS", "ID_OBJECT"))
        .values().toArray
        .map(json => {
          val jobj = JSON.parseObject(json.toString)
          (jobj.getString("ip"), jobj.getString("user"))
        }).toMap
    } catch {
      case e: Exception => {
        maps = maps
        logger.error("getUserName", e)
        //        e.printStackTrace()
      }
    }
    maps
  }

  /**
   * 获取Top用户相关信息
   */
  def getUserInfo(baseRDD: RDD[DnsBean], userMap: Map[String, String], phoneMap: Map[String, String], time: Long,
                  businessIpRules: Array[(Long, Long, String, String, String)],
                  inNetRule: Array[(Long, Long, String)], sc: SparkContext) = {
    //ES报表数据类型
    val types = "user"
    //暂时只计算客户名等于华数杭网的数据
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val last_key = s"dns:flow_top_dd:${df.format((time - 86400) * 1000).substring(0, 10)}"
    val redis_key = s"dns:flow_top_dd:${df.format(time * 1000).substring(0, 10)}"

    val dnsUserInfo = baseRDD
      //整理数据 计算内网数 错误数 获取用户名
      .map(bean => {
        ((bean.clientName, bean.clientIp, bean.domain, bean.aip), (bean.resolver, bean.error))
      }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter(_._1._1 == 1)
      .filter(_._2._1 > 50)
      .sortBy(_._2._1, ascending = false)
      .take(20000)

    val array = dnsUserInfo.map(top => {
      //客户名
      val userName = userMap.getOrElse(top._1._2, "")
      //内网
      var inNet = 0L
      val longIp = IpUtil.ipToLong(top._1._4)
      //匹配大表
      inNetRule.foreach(tuple => {
        if (tuple._1 <= longIp && tuple._2 >= longIp) {
          inNet = top._2._1
        } else {
          //匹配小表
          businessIpRules.foreach(rule => {
            if (rule._1 <= longIp && rule._2 >= longIp) {
              inNet = top._2._1
            }
          })
        }
      })
      ((top._1._1, userName, top._1._3, top._1._2), (top._2._1, top._2._2, inNet))
    })
    val rdd1 = sc.parallelize(array.sortBy(-_._2._2).take(100))

    val userInfoes = rdd1.map(per => {
      val addressAndPhone = phoneMap.getOrElse(per._1._2, " " + "\t" + " ").split("\t")
      val accesstime = df.parse(df.format(time * 1000)).getTime / 1000
      //      types: String, clientName: Int, clientIp: String, userName: String, phone: String, address: String, domain: String,
      //      number: Long, accesstime: Long, error: Long, inNet: Long
      EsDnsUserInfo(types, per._1._1, per._1._4, per._1._2, addressAndPhone(1), addressAndPhone(0), per._1._3,
        per._2._1, accesstime, per._2._2, per._2._3)
    })

    EsSpark.saveToEs(userInfoes, s"bigdata_dns_flow_top_user_${df.format(new Date()).substring(0, 4)}/aip")

    val rdd2 = sc.parallelize(array)

    val result = rdd2.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .mapPartitions(per => {
        var res = new scala.collection.mutable.ListBuffer[EsDnsUserInfoDd]()
        val jedisPool = JedisPool.getJedisPool()
        val jedis = JedisPool.getJedisClient(jedisPool)
        val pipe = jedis.pipelined()
        try {

          var redisMap: Map[String, Response[String]] = Map[String, Response[String]]()
          while (per.hasNext) {
            val line: ((Int, String, String, String), (Long, Long, Long)) = per.next()
            val accesstime = df.parse(df.format(time * 1000).substring(0, 10).concat(" 00:00:00")).getTime / 1000
            val uk = line._1._1 + line._1._2 + line._1._3 + line._1._4
            val day = df.format(accesstime * 1000).substring(0, 10).replaceAll("-", "")
            val key = s"${day}_${DNSUtils.MD5Encode(uk)}"

            redisMap += (key -> pipe.hget(redis_key, key))
            val addressAndPhone = phoneMap.getOrElse(line._1._2, " " + "\t" + " ").split("\t")

            val bean = EsDnsUserInfoDd(types, line._1._1, line._1._4, line._1._2, addressAndPhone(1), addressAndPhone(0), line._1._3,
              line._2._1, accesstime, line._2._2, line._2._3, key)
            res += bean
          }
          pipe.sync()

          res.foreach(bean => {
            val value = redisMap.get(bean.key)
            if (value.get.get() != null) {
              val redisEsDnsUserInfoDd = util.JsonTrans.Json2EsDnsUserInfoDd(value.get.get())
              if (redisEsDnsUserInfoDd != null) {
                if (redisEsDnsUserInfoDd.number != null) {
                  bean.number += redisEsDnsUserInfoDd.number
                }
                if (redisEsDnsUserInfoDd.inNet != null) {
                  bean.inNet += redisEsDnsUserInfoDd.inNet
                }
                if (redisEsDnsUserInfoDd.error != null) {
                  bean.error += redisEsDnsUserInfoDd.error
                }
              }
            }
            pipe.hset(redis_key, bean.key, util.JsonTrans.EsDnsUserInfoDd2Json(bean))
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
          if (jedis.exists(last_key)) {
            jedis.del(last_key)
          }
          if (jedis != null) {
            jedis.close()
          }
          if (jedisPool != null) {
            jedisPool.destroy()
          }
        }
        res.iterator
      })
    EsSpark.saveToEs(result, dnsTopDd, Map("es.mapping.id" -> "key"))
  }

  /**
   * Aip业务聚合
   */
  def getAipBusiness(baseTopRdd: RDD[DnsBeanTop], time: Long) = {
    //ES报表数据类型
    val types = "business"

    val businessRDD = baseTopRdd.map(bean =>
      (bean.clientName, bean.resourceName, bean.resourceType, bean.resourceProps, bean.resolver))

    //全量数据
    val allAipBusiness = businessRDD.map(tuple => ((tuple._2, tuple._3, tuple._4), tuple._5))
      .reduceByKey(_ + _)
      .map(reduced =>
        EsDnsAipBusiness(0, types, reduced._1._1, reduced._1._2, reduced._1._2 + "/" + reduced._1._3, reduced._2, time))
    EsSpark.saveToEs(allAipBusiness, dnsTop)

    //客户维度数据
    val clientAipBusiness = businessRDD.map(tuple => ((tuple._1, tuple._2, tuple._3, tuple._4), tuple._5))
      .reduceByKey(_ + _)
      .map(reduced =>
        EsDnsAipBusiness(reduced._1._1, types, reduced._1._2, reduced._1._3, reduced._1._3 + "/" + reduced._1._4, reduced._2, time))
    EsSpark.saveToEs(clientAipBusiness, dnsTop)
  }


  /**
   * DNS服务器解析次数排名
   */
  def getDnsServerTopN(baseRDD: RDD[DnsBean], time: Long) = {
    //ES报表数据类型
    val types = "dnsIp"

    //全部数据
    val allDnsIpTopN = baseRDD.map(bean => (bean.dnsIp, 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsServerTop(0, types, reduced._1, reduced._2, time))
    EsSpark.saveToEs(allDnsIpTopN, dnsTop)

    //客户维度数据
    val clientDnsIpTopN = baseRDD.map(bean => ((bean.clientName, bean.dnsIp), 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsServerTop(reduced._1._1, types, reduced._1._2, reduced._2, time))
    EsSpark.saveToEs(clientDnsIpTopN, dnsTop)
  }


  /**
   * Aip省份
   */
  def getProvinceNum(baseRDD: RDD[DnsBean], time: Long) = {
    //ES报表数据类型
    val types = "province"

    //全量数据
    val allAipProvince = baseRDD.map(bean => (bean.province, 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsAipProvince(0, types, reduced._1, reduced._2, time))
    EsSpark.saveToEs(allAipProvince, dnsTop)

    //客户维度数据
    val clientAipProvince = baseRDD.map(bean => ((bean.clientName, bean.province), 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsAipProvince(reduced._1._1, types, reduced._1._2, reduced._2, time))
    EsSpark.saveToEs(clientAipProvince, dnsTop)
  }


  /**
   * Aip运营商
   */
  def getOperatorTopN(baseRDD: RDD[DnsBean], time: Long) = {
    //ES报表数据类型
    val types = "operator"

    //全量数据
    val allOperatorTopN = baseRDD.map(bean => (bean.operator, 1L))
      .reduceByKey(_ + _)
      .map(reduced => EsDnsAipOperator(0, types, reduced._1, reduced._2, time))
    EsSpark.saveToEs(allOperatorTopN, dnsTop)

    //客户维度数据
    val clientOperatorTopN = baseRDD.map(bean => ((bean.clientName, bean.operator), 1L))
      .reduceByKey(_ + _)
      .map(reduced =>
        EsDnsAipOperator(reduced._1._1, types, reduced._1._2, reduced._2, time))
    EsSpark.saveToEs(clientOperatorTopN, dnsTop)
  }


  /**
   * 计算DNS每五分钟的QPS
   */
  def getDnsQps(baseRDD: RDD[DnsBean], time: Long) = {
    val types = "qps"
    //全量数据
    val allQps = baseRDD.map(per => (types, (1L, per.error)))
      .reduceByKey((a, b) => {
        val num = a._1 + b._1
        val error = a._2 + b._2
        (num, error)
      })
      .map(rdd => EsDnsQps(0, rdd._1, rdd._2._1, rdd._2._2, Math.floor(rdd._2._1 / 300).toLong, time, time + "qps_0"))
    EsSpark.saveToEs(allQps, newDnsRatioIndex, Map("es.mapping.id" -> "key"))


    //客户维度数据
    val clientQps = baseRDD.map(bean => (bean.clientName, (1L, bean.error)))
      .reduceByKey((a, b) => {
        val num = a._1 + b._1
        val error = a._2 + b._2
        (num, error)
      })
      .map(rdd => EsDnsQps(rdd._1, types, rdd._2._1, rdd._2._2, Math.floor(rdd._2._1 / 300).toLong, time, time + "qps_" + rdd._1))
    EsSpark.saveToEs(clientQps, newDnsRatioIndex, Map("es.mapping.id" -> "key"))
  }

  /**
   * 客户名 客户IP 域名 dns服务器 aip 五维度聚合
   */
  def dnsRpt(baseRDD: RDD[DnsBean], sc: SparkContext, businessIpRules: Array[(Long, Long, String, String, String)], domains: Array[String]): RDD[DnsBeanTop] = {

    val sorted = baseRDD.map(bean => {
      ((bean.clientName, bean.domain, bean.dnsIp, bean.aip), (bean.resolver, bean.error))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(per => {
        val clientName = per._1._1
        val domain = per._1._2
        val dnsIp = per._1._3
        val aip = per._1._4
        var resolver = per._2._1
        val error = per._2._2
        if (domains.contains(domain)) {
          resolver += 10
        }
        ((clientName, domain, dnsIp, aip), (resolver, error))
      })
      //过滤条件
      .filter(_._2._1 >= 10)
      .sortBy(_._2._1, ascending = false)
      .take(70000)

    val rdd = sorted.map(tuple => {
      var resolver = tuple._2._1
      if (domains.contains(tuple._1._2)) {
        resolver -= 10
      }
      val longIp = Utils.ipToLong(tuple._1._4)
      //资源名称 资源类型 资源属性
      var resourceName = ""
      var resourceType = ""
      var resourceProps = ""
      businessIpRules.foreach(rule => {
        if (rule._1 <= longIp && rule._2 >= longIp) {
          resourceName = rule._3
          resourceType = rule._4
          resourceProps = rule._5
        }
      })
      DnsBeanTop(tuple._1._1, tuple._1._2, tuple._1._3, tuple._1._4, resolver, tuple._2._2, longIp, resourceName, resourceType, resourceProps)
    })

    val resultRDD: RDD[DnsBeanTop] = sc.parallelize(rdd)
    resultRDD
  }


  /**
   * 计算响应代码占比
   */
  def getResponseCodeRatio(baseRDD: RDD[DnsBean], time: Long) = {
    // ES报表数据类型
    val types = "responseCode"

    //全量数据响应代码占比
    val allResponseCodeNum = baseRDD.map(bean => (bean.responseCode, 1))
      .reduceByKey(_ + _)
      .map(line => EsResponseCode(0, line._1, line._2, types, time))
    EsSpark.saveToEs(allResponseCodeNum, newDnsRatioIndex)

    //客户数据响应代码占比
    val clientResponseCodeNum = baseRDD.map(bean => ((bean.clientName, bean.responseCode), 1))
      .reduceByKey(_ + _)
      .map(line => EsResponseCode(line._1._1, line._1._2, line._2, types, time))
    EsSpark.saveToEs(clientResponseCodeNum, newDnsRatioIndex)
  }

  /**
   * 各CODE top 域名
   *
   * @param baseRDD
   * @param time
   */
  def getResponseCodeDomainRatio(baseRDD: RDD[DnsBean], time: Long) = {
    // ES报表数据类型
    val types = "responseCodeDomain"
    val top = 5000
    // 全量数据响应代码占比

    // 域名维度
    val allResponseCodeNum: RDD[EsResponseCodeDomain] = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.domain), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3), bean._2))
      .reduceByKey(_ + _)
      .map(bean => (bean._1._1, (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeDomain(0, tuple._1, tk._1, tk._2, types, time))
      })
    val month = new SimpleDateFormat("yyyyMM").format(new Date().getTime)
    val dnsRatioMonthIndex = s"bigdata_dns_flow_ratio_${month}/ratio"
    EsSpark.saveToEs(allResponseCodeNum, dnsRatioMonthIndex)

    // 泛域名维度
    val allResponseCodeNumAuthority = allResponseCodeNum
      .map(bean => ((bean.responseCode, Utils.domian2Authority(bean.domain)), bean.number))
      .reduceByKey(_ + _)
      .map(bean => (bean._1._1, (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeAuthorityDomain(0, tuple._1, tk._1, tk._2, "responseCodeAuthorityDomain", time))
      })
    EsSpark.saveToEs(allResponseCodeNumAuthority, dnsRatioMonthIndex)

    // 客户数据响应代码占比

    // 域名维度
    val clientlResponseCodeNum = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.domain, bean.clientName), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3, bean._1._4), bean._2))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._1, bean._1._3), (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeDomain(tuple._1._2, tuple._1._1, tk._1, tk._2, types, time))
      })
    EsSpark.saveToEs(clientlResponseCodeNum, dnsRatioMonthIndex)

    // 泛域名维度
    val clientResponseCodeNumAuthority = clientlResponseCodeNum
      .map(bean => ((bean.responseCode, Utils.domian2Authority(bean.domain), bean.clientName), bean.number))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._1, bean._1._3), (bean._1._2, bean._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(top)
        topk.map(tk => EsResponseCodeAuthorityDomain(tuple._1._2, tuple._1._1, tk._1, tk._2, "responseCodeAuthorityDomain", time))
      })
    EsSpark.saveToEs(clientResponseCodeNumAuthority, dnsRatioMonthIndex)

  }

  /**
   * 各CODE下top 用户
   *
   * @param baseRDD
   * @param time
   */
  def getResponseCodeClientIPRatio(baseRDD: RDD[DnsBean], time: Long) = {
    // ES报表数据类型
    val types = "responseCodeClientIP"
    //全量数据响应代码占比
    val allResponseCodeNum = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.clientIp), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3), bean._2))
      .reduceByKey(_ + _)
      .map(line => (line._1._1, (line._1._2, line._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(500)
        topk.map(tk => EsResponseCodeClientIP(0, tuple._1, tk._1, tk._2, types, time))
      })
    val month = new SimpleDateFormat("yyyyMM").format(new Date().getTime)
    val dnsRatioMonthIndex = s"bigdata_dns_flow_ratio_${month}/ratio"
    EsSpark.saveToEs(allResponseCodeNum, dnsRatioMonthIndex)

    //客户数据响应代码占比
    val clientResponseCodeNum = baseRDD
      .map(bean => ((random.nextInt(100), bean.responseCode, bean.clientIp, bean.clientName), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._2, bean._1._3, bean._1._4), bean._2))
      .reduceByKey(_ + _)
      .map(line => ((line._1._1, line._1._3), (line._1._2, line._2)))
      .groupByKey()
      .flatMap(tuple => {
        val topk = tuple._2.toArray.sortBy(-_._2).take(500)
        topk.map(tk => EsResponseCodeClientIP(tuple._1._2, tuple._1._1, tk._1, tk._2, types, time))
      })
    EsSpark.saveToEs(clientResponseCodeNum, dnsRatioMonthIndex)

  }

  /**
   * 计算响应类型占比
   */
  def getResponseTypeRatio(baseRDD: RDD[DnsBean], time: Long) = {
    //ES报表数据类型
    val types = "responseType"

    //全量数据请求类型占比
    val allResponesNum = baseRDD.map(bean => ((random.nextInt(100), bean.responseType), 1))
      .reduceByKey(_ + _)
      .map(bean => (bean._1._2, bean._2))
      .reduceByKey(_ + _)
      .map(line => BeanObj.EsResponseType(0, line._1, line._2, types, time))
    EsSpark.saveToEs(allResponesNum, newDnsRatioIndex)

    //客户数据请求类型占比
    val clientResponseNum = baseRDD.map(bean => ((bean.clientName, bean.responseType, random.nextInt(100)), 1))
      .reduceByKey(_ + _)
      .map(bean => ((bean._1._1, bean._1._2), bean._2))
      .reduceByKey(_ + _)
      .map(line => BeanObj.EsResponseType(line._1._1, line._1._2, line._2, types, time))
    EsSpark.saveToEs(clientResponseNum, newDnsRatioIndex)
  }


  /**
   * 计算请求类型占比
   */
  def getRequestTypeRatio(baseRDD: RDD[DnsBean], time: Long) = {
    //ES报表数据类型
    val types = "requestType"

    //全量数据请求类型占比
    val allRequestNum = baseRDD.map(bean => (bean.requestType, 1))
      .reduceByKey(_ + _)
      .map(line => BeanObj.EsRequestType(0, line._1, line._2, types, time))
    EsSpark.saveToEs(allRequestNum, newDnsRatioIndex)

    //客户数据请求类型占比
    val clientRequestNum = baseRDD.map(bean => ((bean.clientName, bean.requestType), 1))
      .reduceByKey(_ + _)
      .map(line => BeanObj.EsRequestType(line._1._1, line._1._2, line._2, types, time))
    EsSpark.saveToEs(clientRequestNum, newDnsRatioIndex)
  }

  /**
   * 读取Mysql业务分组 资源类型=1  资源属性=2 资源名称=3 IP匹配规则
   */
  def getBusinessIpRules(spark: SQLContext) = {
    //导入隐式转换
    import spark.implicits._
    val businessRules = Utils.ReadMysql(spark, "dns_ip_segment_detail")
      .map(row => {
        val minIp = row.getAs[Long]("min_long_ip")
        val maxIp = row.getAs[Long]("max_long_ip")
        val resourceName = row.getAs[Int]("s_id").toString
        val resoueceType = row.getAs[Int]("company_id").toString
        val resourceProps = row.getAs[Int]("resource_id").toString
        (minIp, maxIp, resourceName, resoueceType, resourceProps)
      }).rdd.collect()
    businessRules
  }

  /**
   * 读取Mysql 获取aip击中内网网段ip段
   */
  def getInNetIpRule(spark: SQLContext): Array[(Long, Long, String)] = {
    //导入隐式转换
    import spark.implicits._
    val ipRule = Utils.ReadMysql(spark, "dns_media")
      .map(row => {
        val min_long_ip = row.getAs[Long]("min_long_ip")
        val max_long_ip = row.getAs[Long]("max_long_ip")
        val media_type = row.getAs[String]("media_type")
        (min_long_ip, max_long_ip, media_type)
      }).rdd.collect()
    ipRule
  }

  /**
   * 读取MySQL客户名IP规则数据
   */
  def getClientName(spark: SQLContext): Array[(Long, Long, Int)] = {
    //导入隐式转换
    import spark.implicits._
    val ipRDD = Utils.ReadMysql(spark, "dns_client_detail")
      .map(row => {
        val min_long_ip = row.getAs[Long]("min_long_ip")
        val max_long_ip = row.getAs[Long]("max_long_ip")
        val media_type = row.getAs[Int]("client_type_id")
        (min_long_ip, max_long_ip, media_type)
      }).rdd.collect()
    ipRDD
  }

  /**
   * 读取域名白名单数据
   */
  def getDomainWhitelist(spark: SQLContext): Array[String] = {
    //导入隐式转换
    import spark.implicits._
    val domains = Utils.ReadMysql(spark, "dns_white_domain")
      .map(row => {
        val domain = row.getAs[String]("domain")
        domain
      }).rdd.collect()
    domains
  }


  /**
   * 整理基本数据
   */
  def getBaseRDD(rdd: RDD[String], random: Random, clientIpRule: Array[(Long, Long, Int)], appTime: Long): RDD[DnsBean] = {

    rdd.mapPartitions(itr => {
      var dnsBeanBuffer = new scala.collection.mutable.ListBuffer[DnsBean]()
      itr.foreach(str => {
        var jobj: JSONObject = null
        try {
          jobj = JSON.parseObject(str)
        } catch {
          case e: Exception => {
            logger.error("JSON.parseObject, " + str, e)
          }
        }
        if (jobj != null) {
          val domain = jobj.getString("Domain")
          val logTimestamp = jobj.getLong("Timestamp")
          val serverIP = jobj.getString("ServerIP")
          val QR = jobj.getBoolean("QR")
          if (StringUtils.isNotEmpty(domain) && !domain.contains("master01")
            && !domain.contains(".localdomain")
            && !domain.contains(" ")
            && (!domain.contains("DHCP") || !domain.substring(domain.length - 4, domain.length).equals("DHCP"))
            && !domain.contains("HOST")
            && !domain.contains("Relteak")
            && !domain.contains("getCached")
            && !domain.contains("BlinkAP")
            && logTimestamp >= appTime && logTimestamp < appTime + 1000 * 60 * 5 //时间过滤
            && util.IpUtil.isInRange(serverIP, "218.108.248.192/26")
            && QR == true
          ) {


            try {
              val dnsBean = DnsBean()

              /**
               * 请求类型
               * A,AAAA,NS,CNAME,DNAME,MX,TXT
               */
              dnsBean.requestType = jobj.getString("Type")
              /**
               * Answers字段信息
               */
              val answers = jobj.getJSONArray("Answers")

              /**
               * 获取响应代码
               * 0 -> NOERROR  成功的响应，这个域名解析成功
               * 2 -> SERVFAIL 失败的响应，这个域名的权威服务器拒绝响应或者响应REFUSE
               * 3 -> NXDOMAIN 不存在的记录，这个具体的域名在权威服务器中并不存在
               * 5 -> REFUSE   拒接，这个请求源IP不在服务的范围内
               */
              dnsBean.responseCode = jobj.getInteger("ResponseCode")
              //错误数 responseCode非0都是解析错误  || !requestType.equals("A")
              if (dnsBean.responseCode != 0 || answers.isEmpty) {
                dnsBean.error = 1
              }
              //域名
              dnsBean.domain = domain
              //DNS服务器
              dnsBean.dnsIp = serverIP

              /**
               * 获取客户名
               */
              dnsBean.clientIp = jobj.getString("ClientIP")
              val clientLong = Utils.ipToLong(dnsBean.clientIp)
              dnsBean.clientName = clientIpRule.find(tp => tp._1 <= clientLong && tp._2 >= clientLong)
                .headOption.getOrElse((0, 0, 5))._3

              /**
               * 解析Aip responseType
               *
               * 如果请求类型为A 解析Answers Aip随机取 responseType取aip对应的Type
               * 如果请求类型不为A 不解析Answers responseType:other
               */
              if ("A".equalsIgnoreCase(dnsBean.requestType)) {
                //Answers可能是""
                if (answers.size() > 0) {
                  var aIpTp = new collection.mutable.ListBuffer[String]()
                  var responseTypeTp = new collection.mutable.ListBuffer[String]()
                  //遍历answers数组  数组里嵌套的是json
                  for (index <- 0 to answers.size() - 1) {
                    val jobj = answers.getJSONObject(index)
                    val answerData = jobj.getString("Type")
                    responseTypeTp += answerData
                    if (answerData.equals("A")) aIpTp += jobj.getString("Value")
                  }
                  if (aIpTp.length > 0) dnsBean.aip = aIpTp(random.nextInt(aIpTp.length))
                  if (responseTypeTp.length > 0) dnsBean.responseType = responseTypeTp.last
                }
              }

              /**
               * 运营商  省份
               */
              val maps = IpSearch.getRegionByIp(dnsBean.aip)
              if (!maps.isEmpty) {
                dnsBean.operator = maps.get("运营").toString
                dnsBean.province = maps.get("省份").toString
              }

              if ("0.0.0.0".equals(dnsBean.aip)) dnsBean.error = 1

              //日志时间
              dnsBean.logTimestamp = jobj.getLong("Timestamp")
              dnsBeanBuffer += dnsBean
            } catch {
              case e: Exception => {
                logger.error("getBaseRDD", e)
              }
            }
            //  DnsBean(clientName, responseCode, requestType, dnsIp, aIp, domain, resolver, error, operator, province, responseType, clientIP, logTimestamp)
          }
        }
      })
      dnsBeanBuffer.iterator
    })
  }

  def call(spark: SQLContext, sparkSession: SparkSession, sc: SparkContext, timestamp: Long): Unit = {

    val paths = new ListBuffer[String]()
    var endTime = 0L
    //多取前一个文件， 部分记录在前一个文件中
    //目录数据样本 hdfs://30.250.60.7/dns_log/2019/12/25/1621_1577262060
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    for (w <- 10 * 60 to 5 * 60 by -step) {
      endTime = timestamp - w * 1000
      val path = s"/dns_log/${
        Utils.timestamp2Date("yyyy/MM/dd/HHmmss", endTime - step * 1000)
      }_${
        endTime / 1000 - step
      }"

      try {
        if (hdfs.exists(new Path(path))) {
          //        if (w >= 580) {
          paths += path
        } else {
          logger.info(s"file not exists ${path}")
        }
      } catch {
        case e: Exception => {
          logger.error(s"FileException ${path}", e)
        }
      }
    }

    if (paths.size == 0) {
      logger.info("paths size is  0,  empty ..... return ")
      return
    }

    val dnsPaths = paths.mkString(",")
    val beginTime = endTime - 1000 * 60 * 5
    val now = Utils.timestamp2Date("yyyyMMddHHmm", beginTime)

    /**
     * es 时间精确到秒
     */
    endTime = endTime / 1000

    //    logger.info("读取数据...")
    val dns_log = sc.textFile(dnsPaths).coalesce(180)

    var baseRDD: RDD[BeanObj.DnsBean] = null
    var baseTopRdd: RDD[BeanObj.DnsBeanTop] = null
    try {
      //创建Redis连接
      logger.info("init jedis...")
      val jedisPool = JedisPool.getJedisPool()
      val jedis: Jedis = JedisPool.getJedisClient(jedisPool)
      //读取Mysql客户名ip规则数据
      logger.info("读取Mysql客户名ip规则数据...")
      val clientIpRule = getClientName(spark)
      //读取Mysql域名白名单数据
      logger.info("读取Mysql域名白名单数据...")
      val domains = getDomainWhitelist(spark)
      //读取aip击中内网ip规则数据
      logger.info("读取aip击中内网ip规则数据...")
      val inNetRule = getInNetIpRule(spark)
      //读取aip业务网段
      logger.info("读取aip业务网段...")
      val businessIpRules = getBusinessIpRules(spark)
      //读取redis中clear当日统计数据
      //      logger.info("读取redis中clear当日统计数据...")
      //      val redisClear: Map[String, EsDataBeanDd] = getClearAggreate(jedis, now.substring(0, 8))
      //读取Redis在线用户表
      logger.info("读取Redis在线用户表...")
      val userMap = getUserName(jedis)
      val userMapValue: Broadcast[Map[String, String]] = sc.broadcast(userMap)
      //读取Redis详细信息
      logger.info("读取Redis详细信息...")
      val phoneAndAddress = getPhoneNumAndAddress(jedis)
      val phoneAndAddressValue: Broadcast[Map[String, String]] = sc.broadcast(phoneAndAddress)

      //读取权威域名详细信息
      logger.info("读取权威域名详细信息...")
      val authDomainMsgs = getAuthorityDomainMessage(jedis)
      val authDomainMsgsValue: Broadcast[Array[authDomainMsg]] = sc.broadcast(authDomainMsgs)

      //生成随机数
      //      val random: Random = new Random()

      //读取数据


      //用fastjson解析json数据
      //      var paseJson = JSON.parseObject("")
      //      val jobj = dns_log.map(line => {
      //        try {
      //          paseJson = JSON.parseObject(line)
      //        } catch {
      //          case e: Exception => {
      //            logger.error("JSON.parseObject, " + line, e)
      //          }
      //        }
      //        paseJson
      //      })


      //TODO 整理原始数据 返回RDD[DnsBean] 并把数据持久化到内存+磁盘
      baseRDD = getBaseRDD(dns_log, random, clientIpRule, beginTime)
        //        .filter(date => date.logTimestamp >= hdfsLogTimestamp && date.logTimestamp < hdfsLogTimestamp + 1000 * 60 * 5)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      logger.info("dnsRDD.partitions.size: {} ", baseRDD.partitions.size);

      //      val baseRDD = dnsRDD.filter(date => date.logTimestamp >= hdfsLogTimestamp && date.logTimestamp < hdfsLogTimestamp + 1000 * 60 * 5)

      //      user domain resolver聚合后写入hdfs
      logger.info("SaveUserInfo2HDFS...")
      SaveUserInfo2HDFS(baseRDD, userMapValue.value, now)

      //TODO 1.0 qps
      logger.info("getDnsQps...")
      getDnsQps(baseRDD, endTime)

      //TODO 1.1 请求类型占比
      logger.info("请求类型占比...")
      getRequestTypeRatio(baseRDD, endTime)


      //TODO 1.2 响应类型占比
      logger.info("getResponseTypeRatio...")
      getResponseTypeRatio(baseRDD, endTime)


      //TODO 1.2 响应代码占比
      logger.info("getResponseCodeRatio...")
      getResponseCodeRatio(baseRDD, endTime)

      //响应代码top域名
      logger.info("响应代码top域名...")
      getResponseCodeDomainRatio(baseRDD, endTime)
      //响应代码top clientip
      logger.info("响应代码top clientip...")
      getResponseCodeClientIPRatio(baseRDD, endTime)

      //TODO 1.3 计算维度统计数据
      logger.info("计算维度统计数据...")
      baseTopRdd = dnsRpt(baseRDD, sc, businessIpRules, domains)
      //持久化Top数据
      baseTopRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

      //TODO 1.3.0 Top数据写入ES
      logger.info("Top数据写入ES...")
      val detailReportRDD = Tuple2Es(baseTopRdd, inNetRule, endTime, businessIpRules, authDomainMsgsValue.value, now)

      //报表数据写入ES
      logger.info("报表数据写入ES...")
      getDetailReport(detailReportRDD, endTime, now)


      //TODO 1.3.1 计算用户相关信息
      logger.info("计算用户相关信息...")
      getUserInfo(baseRDD, userMapValue.value, phoneAndAddressValue.value, endTime, businessIpRules, inNetRule, sc)

      //TODO 1.4 Aip运营商排名
      logger.info("Aip运营商排名...")
      getOperatorTopN(baseRDD, endTime)


      //TODO 1.5 Aip省份计算
      logger.info("Aip省份计算...")
      getProvinceNum(baseRDD, endTime)

      //TODO 1.6 DNS服务器解析次数排名
      logger.info("DNS服务器解析次数排名...")
      getDnsServerTopN(baseRDD, endTime)

      //TODO 1.7 Aip业务
      logger.info("Aip业务...")
      getAipBusiness(baseTopRdd, endTime)

      jedis.close()
      jedisPool.destroy()

    } catch {
      case e: FileNotFoundException => {
        logger.error("FileNotFoundException", e)
      }
      case e: IllegalArgumentException => {
        logger.error("IllegalArgumentException", e)
      }
    } finally {
      //把持久化的数据释放掉
      if (baseRDD != null) {
        baseRDD.unpersist()
        logger.info("dnsRDD.unpersist...")
      }
      if (baseTopRdd != null) {
        baseTopRdd.unpersist()
        logger.info(" baseTopRdd.unpersist...")
      }
    }

    logger.info("-----------------------------[批处理结束]")
  }

  def main(args: Array[String]): Unit = {
    val properties = DNSUtils.loadConf()
    val conf = new SparkConf()
    conf.setAppName("SparkStreamingReadHDFS")
      //    conf.setMaster("local[*]")
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
    val ssc = new StreamingContext(sparkContext, Seconds(300))
    val stream = ssc.textFileStream("hdfs://nns/DontDelete")
    stream.foreachRDD((rdd, time) => {
      logger.info("task start ..... batch time:{}", time)
      //      获取当前时间
      call(spark, sparkSession, sparkContext, time.milliseconds)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def test(sc: SparkContext): Unit = {

    val aa = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 11, 8, 181))


    //     aggregateClear(resultRDD, sc, );

    val result = aa.coalesce(2)
      .mapPartitions(par => {
        val jedis = JedisPool.getJedisClient()
        val pipe = jedis.pipelined()
        //        pipe.multi()
        var res = List[(Int, Int)]()
        var redisMap: Map[String, Response[String]] = Map[String, Response[String]]()
        val redisKey = "test:partitions"

        while (par.hasNext) {
          val id = par.next()
          //          logger.info("id: {}", id)
          var response: Response[String] = pipe.hget(redisKey, id.toString)
          redisMap += (id.toString -> response)

          //          var nv = 0
          //          if (value != null && value.get() != null) {
          //            nv = value.get().toInt
          //            logger.info(nv.toString)
          //          }
          //          pipe.hset(redisKey, id.toString, (nv + 1).toString)
          //          pipe.sync()

          res.::=(id, 1)
        }
        //        val rs = pipe.syncAndReturnAll()
        pipe.sync()
        res = res.map(item => {
          val obj = redisMap.get(item._1.toString)
          if (obj.nonEmpty) {

          }
          var value = 0;
          if (obj.get.get() == null) {

            logger.info(" is null, , {}", item._1)
          } else {
            value = obj.get.get().toInt
            logger.info("id:{}, obj:{}", item.toString, value)
          }

          //          item._2 = item._2 + value

          //          pipe.hset(obj.get.get().toInt)
          (item._1, item._2 + value)
        })
        pipe.close()
        jedis.close()
        res.iterator
      })


    logger.info("{}，{}", result.count(), result.top(20))


    //    val ap = EsDataBeanDd("a", 1, "b", "", "", "", 0l, 0l, 0l, 0l, "", "", "", "", "", "", "", "", "", "", "", 0l, 0l, 0l, 0l, "", 0L)
    //
    //    val jsonString = util.JsonTrans.EsDataBeanDd2Json(ap)
    //
    //    logger.info("json string:{}", jsonString)
    //    logger.info("json object:{}", util.JsonTrans.Json2EsDataBeanDd(jsonString))

  }


}
