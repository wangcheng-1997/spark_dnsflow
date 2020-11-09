package com.zjtuojing.dnsflow

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ThreadLocalRandom

import com.alibaba.fastjson.JSON
import com.zjtuojing.dnsflow.BeanObj.{FlowmsgData, UserRptBean}
import com.zjtuojing.utils.DNSUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, SQL}

import scala.collection.mutable.ArrayBuffer

/**
 * 用户活跃度分析报表
 */
object LivenessRpt {


  /**
   * 查询Redis 获取地区编号
   */
  def getDistrictNum(jedis: Jedis, index: String) = {
    val maps = jedis.hgetAll(index).values().toArray()
      .map(line => {
        val obj = JSON.parseObject(line.toString)
        val username = obj.getString("userName")
        val rid1 = obj.getIntValue("rid1")
        val rid2 = obj.getIntValue("rid2")
        val rid3 = obj.getIntValue("rid3")
        val rid4 = obj.getIntValue("rid4")
        (username, (rid1, rid2, rid3, rid4))
      }).toMap
    maps
  }

  /**
   * 获取App Name
   */
  def getAppName(ip: Long, appBro: List[(Long, Long, String)]): String = {
    var appName = "其他"
    appBro.foreach(bro => {
      val start = bro._1
      val end = bro._2
      if (start <= ip && end > ip) {
        appName = bro._3
      }
    })
    appName
  }

  /**
   * 根据域名获取标签
   */
  def getDomainTags(domian: String, broadcastTags: Broadcast[Map[String, String]], broadcastBaseTags: Broadcast[Map[String, String]]): (String, String, String) = {
    val broadcastMaps = broadcastTags.value
    //打标签
    val tagFiles = broadcastMaps.getOrElse(domian, "未知|未知|未知").split("\\|")
    var oneLabel = "未知"
    var twoLabel = "未知"
    var threeLabel = "未知"
    try {
      oneLabel = tagFiles(0)
      twoLabel = tagFiles(1)
      threeLabel = tagFiles(2)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    //Redis key-cache-liveness-domain-label域名匹配不上 从broadband-probe-cache-dns-authDomain拿权威域名匹配
    if (threeLabel.equals("未知")) {
      //域名转换为权威域名
      val authDomain = Utils.domian2Authority(domian)
      threeLabel = broadcastBaseTags.value.getOrElse(authDomain, threeLabel)
    }
    (oneLabel, twoLabel, threeLabel)
  }

  /**
   * 获取基础表数据 二次匹配使用
   */
  def getRedisAuthDomainTags(jedis: Jedis, index: String): Map[String, String] = {
    //读取Redis基础表数据
    val maps = jedis.hgetAll(index)
      .values()
      .toArray()
      .map(json => {
        val jobj = JSON.parseObject(json.toString)
        val authDomain = jobj.getString("authDomain")
        val websiteName = jobj.getString("websiteName")
        (authDomain, websiteName)
      }).toMap
    maps
  }

  /**
   * 获取标签规则数据
   */
  def getRedisDoaminTags(jedis: Jedis, index: String): Map[String, String] = {
    //读取Redis数据
    val domainMaps = jedis.hgetAll(index)
      .values()
      .toArray()
      .map(json => {
        val jobj = JSON.parseObject(json.toString)
        val domain = jobj.getString("domain")
        val oneLabel = jobj.getString("oneLabel")
        val twoLabel = jobj.getString("twoLabel")
        val threeLabel = jobj.getString("threeLabel")
        (domain, oneLabel + "|" + twoLabel + "|" + threeLabel)
      }).toMap
    domainMaps
  }

  // 自定义去重
  def mydistinct(iter: Iterator[(String, Int)]): Iterator[String] = {
    iter.foldLeft(Set[String]())((CurS, item) => CurS + item._1).toIterator
  }


  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        """
          |com.zjtuojing.dnsflow.LivenessRpt
          |参数错误：
          |
          |<yyyyMMdd>
          |""".stripMargin)
      sys.exit()
    }

    val Array(yesterday) = args

    val properties = DNSUtils.loadConf()
    val logger = LoggerFactory.getLogger(LivenessRpt.getClass)

    //日志时间
    val accesstime = new SimpleDateFormat("yyyyMMdd").parse(yesterday).getTime

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      //      .setMaster("local[*]")
      .set("es.port", properties.getProperty("es2.port"))
      .set("es.nodes", properties.getProperty("es2.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es2.nodes.wan.only"))
      .set("spark.speculation", "true")
      .set("spark.speculation.interval", "5000")
      .set("spark.speculation.quantile", "0.9")
      .set("spark.speculation.multiplier", "2")
      .set("spark.locality.wait","10")
      .set("spark.storage.memoryFraction", "0.3")

    //采用kryo序列化库
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    conf.registerKryoClasses(
      Array(
        classOf[Array[String]], classOf[FlowmsgData], classOf[UserRptBean], classOf[Map[String, Any]]
      )
    )
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    //HADOOP HA
    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))


    //加载scalikejdbc配置文件
    // 加载驱动

    Class.forName("com.mysql.jdbc.Driver")

    // 指定数据库连接url，userName，password

    val url = properties.getProperty("mysql.url")

    val userName = properties.getProperty("mysql.username")

    val password = properties.getProperty("mysql.password")

    ConnectionPool.singleton(url, userName, password)
    DBs.setupAll()


    //TODO 读取redis规则数据
    val jedisPool = JedisPool.getJedisPool()
    val jedis = JedisPool.getJedisClient(jedisPool)
    //把规则表和基础表广播出去
    val tags = getRedisDoaminTags(jedis, "liveness:domain-label")
    val broadcastTags = sc.broadcast(tags)
    val baseTags = getRedisAuthDomainTags(jedis, "dns:dns-authDomain")
    val broadcastBaseTags = sc.broadcast(baseTags)
    //把地区编号表广播出去
    val districtMaps = getDistrictNum(jedis, s"BOSSUSERS:${yesterday}:AAA")
    val boradcastDistrict = sc.broadcast(districtMaps)
    jedis.close()
    jedisPool.destroy()


    //TODO 读取Mysql App规则数据
    var buff = new collection.mutable.ArrayBuffer[(Long, Long, String)]()
    val appRule = DB.readOnly { implicit session =>
      SQL("select * from ip_deploy")
        .map(rs => {
          val appName = rs.string("segment_name").split("\\s")(0)
          val files = rs.string("ip_long_str").split(";")
          for (o <- 0 to files.length - 1) {
            val splits = files(o).split("-")
            buff += ((splits(0).toLong, splits(1).toLong, appName))
          }
          buff
        })
        .list().apply()
    }.flatten

    DBs.closeAll()
    //把数据广播出去
    val appRuleBroadcast = sc.broadcast(appRule)

    var userBaseRDD: RDD[UserRptBean] = null
    var flowmsgBaseRDD: RDD[FlowmsgData] = null

    try{
      //TODO 读取数据(DNS)
      //val userBaseRDD = sc.textFile("/Users/mac/Desktop/dns.log")
      userBaseRDD = sc.textFile(s"hdfs://nns/dns_middle_data/${yesterday}/*/*")
        .map(line => {
          var userName = ""
          var domain = ""
          var num = 1L
          val files = line.split("\001")
          try {
            userName = files(0)
            domain = files(1)
            num = files(2).toLong
          } catch {
            case e: Exception => {
              //e.printStackTrace()
            }
          }
          UserRptBean(userName, domain, num)
        }).filter(_.resolver > 1)
        .filter(!_.domain.contains("null"))
        .filter(!_.domain.equals("local"))
        .filter(!_.domain.contains("HOST"))
        .filter(!_.domain.contains("Relteak"))
        .filter(!_.domain.contains("getCached"))
        .filter(!_.domain.contains("BlinkAP"))
        .filter(!_.domain.contains("master01"))
        .filter(!_.domain.contains(".localdomain"))
        .filter(!_.domain.endsWith("arpa"))
        .coalesce(810, false)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val partitions: Int = userBaseRDD.getNumPartitions

      val simpleDateFormat = new SimpleDateFormat("HH")
      //TODO 读取数据(Flowmsg)
      flowmsgBaseRDD = sc.textFile(s"hdfs://nns/flowmsg_log/${yesterday}/*/*")
        .coalesce(partitions, false)
        .mapPartitions(per => {
          var res = List[FlowmsgData]()
          while (per.hasNext) {
            val line = per.next()

            val Array(timestamp, clientName, sourceIp, destinationIp, bytes) = line.split("\t")
            //匹配app规则数据
            val appName = getAppName(
              Utils.ipToLong(destinationIp), appRuleBroadcast.value
            )
            res.::=(FlowmsgData(simpleDateFormat.format(new Date(timestamp.toLong * 1000)), clientName, sourceIp, appName, bytes.toLong * 2048)) //字节数乘权重值 业务需求
          }
          res.iterator
        }).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val random = ThreadLocalRandom.current()

      //TODO 计算用户关联域名Top10
      val domains = userBaseRDD.map(tuple => ((tuple.userName, tuple.domain), tuple.resolver))
        .reduceByKey(_ + _,270)
        .groupBy(_._1._1)
        .mapValues(itr => {
          itr.toList.sortBy(_._2).reverse.take(10)
        })
        .mapPartitions(per => {
          var res = List[(String, ArrayBuffer[Map[String, Any]])]()
          while (per.hasNext) {
            val line = per.next()
            var domianAndResolve = Map[String, Any]()
            var buff = new ArrayBuffer[Map[String, Any]]()
            val userAndDomainResolve = line._2
            for (index <- 0 to userAndDomainResolve.size - 1) {
              var tagsBuff = new ArrayBuffer[String]()
              domianAndResolve += ("domain" -> userAndDomainResolve(index)._1._2)
              domianAndResolve += ("resolver" -> userAndDomainResolve(index)._2)
              val tags = getDomainTags(userAndDomainResolve(index)._1._2, broadcastTags, broadcastBaseTags)
              tagsBuff += tags._1
              tagsBuff += tags._2
              tagsBuff += tags._3
              //只保留能匹配上的标签
              val distinctedBuff = tagsBuff.distinct -= "未知"
              domianAndResolve += ("tags" -> distinctedBuff)
              buff += domianAndResolve
            }
            res.::=((line._1, buff))
          }
          res.iterator
        })

      //TODO 计算用户全天解析总次数(√)
      val userResolvers = userBaseRDD.map(tuple => (tuple.userName, tuple.resolver))
        .reduceByKey(_ + _,270)

      //TODO 给数据打标签(√)
      //数据去重
      val distincted = userBaseRDD
        .map(line => ((line.userName, line.domain), 1))
        .reduceByKey((a, b) => a,270)
        .map(per => per._1)

      //去重后的数据匹配规则标签
      val userAndDomainTags = distincted.map(tuple => {
        val domainTags = getDomainTags(tuple._2, broadcastTags, broadcastBaseTags)
        (tuple._1, domainTags)
      })
        .filter(line => line._2._3 != "未知")
        .map(line => {
          (line._1, Set(line._2._1, line._2._2, line._2._3))
        })
        .reduceByKey(_ ++ _,270)
        .map(line => (line._1, (line._2 - "未知").toArray))


      //TODO 统计单个用户每天总流量
      val userSumBytes = flowmsgBaseRDD.map(flowRDD => ((flowRDD.clientName, random.nextInt(10000)), flowRDD.bytes))
        .reduceByKey(_ + _)
        .map(per => (per._1._1, per._2))
        .reduceByKey(_ + _)


      //TODO 统计单个用户TOP 10应用的总流量 每个应用流量求和 应用根据目的IP算
      val clientTopN = flowmsgBaseRDD.filter(line => StringUtils.isNotEmpty(line.appName))
        .coalesce(810, false)
        .map(flowRDD => ((flowRDD.clientName, flowRDD.appName), flowRDD.bytes))
        .reduceByKey(_ + _,270)
        .groupBy(_._1._1)
        .map(line => {
          (line._1, line._2.toList.sortBy(-_._2).take(10))
        })
      val userTopApp = clientTopN.map(line => {
        var appAndResolve = Map[String, Any]()
        var topAppBuff = new ArrayBuffer[Map[String, Any]]()
        val userAndAppResolve = line._2
        for (index <- 0 to userAndAppResolve.size - 1) {
          appAndResolve += ("app" -> userAndAppResolve(index)._1._2)
          appAndResolve += ("byte" -> userAndAppResolve(index)._2)
          topAppBuff += appAndResolve
        }
        (line._1, topAppBuff)
      })


      //TODO 统计单个用户全天的流量趋势图 24个小时 每个小时一个采集点
      //求出每个客户名每小时有多少采集点
      val clientTimeAndNumber = flowmsgBaseRDD.map(
        flowRDD =>
          ((flowRDD.clientName, flowRDD.timestamp), flowRDD.sourceIp)
      ).distinct()
        .groupBy(_._1)
        .map(tuple => {
          (tuple._1, tuple._2.toList.size)
        }).collectAsMap()
      val clientTimeAndNexBroadcast = sc.broadcast(clientTimeAndNumber)

      //计算每个用户每小时的流量
      val user24hour = flowmsgBaseRDD.map(flowRDD => {
        ((flowRDD.clientName, flowRDD.timestamp, random.nextInt(10000)), flowRDD.bytes)
      }).reduceByKey(_ + _)
        .map(per => ((per._1._1, per._1._2), per._2)).reduceByKey(_ + _)
        .map(reduced => {
          val nex = clientTimeAndNexBroadcast.value.getOrElse(reduced._1, 1)
          (reduced._1, reduced._2 / nex)
        }).groupBy(_._1._1)
        .map(line => {
          var hoursAndResolve = Map[String, Any]()
          var user24Buff = new ArrayBuffer[Map[String, Any]]()
          val userAndAppResolve = line._2.toList
          for (index <- 0 to userAndAppResolve.size - 1) {
            hoursAndResolve += ("hour" -> userAndAppResolve(index)._1._2.toInt)
            hoursAndResolve += ("byte" -> userAndAppResolve(index)._2)
            user24Buff += hoursAndResolve
          }
          (line._1, user24Buff)
        })


      //TODO 关联数据
      var maps = Map[String, Any]()
      val dnsRDD = domains.cogroup(userResolvers, userAndDomainTags).map(rdd => (rdd._1, rdd))
      val flowmsgRDD = userSumBytes.cogroup(userTopApp, user24hour).map(rdd => (rdd._1, rdd))
      val cogroupRDD = dnsRDD.cogroup(flowmsgRDD)

      val resultRDD = cogroupRDD.map(line => {

        //ES _id
        val id = yesterday + "_" + line._1

        var resolver = 0L
        var sumBytes = 0L
        try {
          resolver = line._2._1.map(_._2._2).flatten.mkString("").toLong
        } catch {
          case e: Exception => {
            //e.printStackTrace()
            //println("resolver ============>" + line._2._1.map(_._2._2).flatten)
          }
        }

        try {
          sumBytes = line._2._2.map(_._2._1).flatten.mkString("").toLong
        } catch {
          case e: Exception => {
            //e.printStackTrace()
            //println("sumBytes ============>" + line._2._2.map(_._2._1).flatten)
          }
        }
        //地区编号
        val districtNums = boradcastDistrict.value.getOrElse(line._1, (0, 0, 0, 0))
        val ints4 = new Array[Int](4)
        ints4(0) = districtNums._1
        ints4(1) = districtNums._2
        ints4(2) = districtNums._3
        ints4(3) = districtNums._4

        maps += ("accesstime" -> accesstime,
          "username" -> line._1,
          "domains" -> line._2._1.map(_._2._1).flatten.flatten.toList,
          "resolver" -> resolver,
          "tags" -> line._2._1.map(_._2._3).flatten.flatten.toList,
          "bytes" -> sumBytes,
          "apps" -> line._2._2.map(_._2._2).flatten.flatten.toList,
          "byte" -> line._2._2.map(_._2._3).flatten.flatten.toList,
          "id" -> id,
          "rids" -> ints4.toBuffer
        )
        maps
      })

      EsSpark.saveToEs(resultRDD, s"bigdata_liveness_${yesterday.substring(0, 6)}/liveness", Map("es.mapping.id" -> "id"))
      //    println(resultRDD.count())
    }catch {
      case e: Exception => {
        //e.printStackTrace()
      }
    }finally {
      userBaseRDD.unpersist()
      flowmsgBaseRDD.unpersist()
    }

    //关闭程序释放资源
    sc.stop()
  }

}
