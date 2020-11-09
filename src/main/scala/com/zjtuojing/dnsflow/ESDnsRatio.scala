package com.zjtuojing.dnsflow

import java.text.SimpleDateFormat
import java.util.Date

import com.zjtuojing.dnsflow.BeanObj._
import com.zjtuojing.utils.DNSUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

/**
 * ClassName ESDnsFTP
 * Date 2020/6/2 17:28
 */
object ESDnsRatio {

  def main(args: Array[String]): Unit = {

    val properties = DNSUtils.loadConf()

    if (args.length != 1) {
      println(
        """
          |参数错误：
          |<taskType hh,6h,dd>
          |""".stripMargin)
      sys.exit()
    }

    val Array(task) = args

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ESAggregate")
//      .setMaster("local[*]")
      .set("es.port", properties.getProperty("es1.port"))
      .set("es.nodes", properties.getProperty("es1.nodes"))
      .set("es.nodes.wan.only", properties.getProperty("es1.nodes.wan.only"))
      .set("es.index.auto.create", properties.getProperty("es1.index.auto.create"))
      .set("spark.es.input.use.sliced.partitions", properties.getProperty("spark.es.input.use.sliced.partitions"))
    //采用kryo序列化库
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册
    sparkConf.registerKryoClasses(
      Array(
        classOf[Array[String]],
        classOf[RatioBeanRequestType],
        classOf[RatioBeanResponseCode],
        classOf[RatioBeanResponseType],
        classOf[RatioBeanQps],
        classOf[RatioBeanResponseCodeDomain],
        classOf[RatioBeanResponseCodeClientIp]
      )
    )
    //      .set("mapping.date.rich", "false")
    //      .set("max.docs.per.partition","100")
    //      .set("spark.es.scroll.size","10000")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    sc.hadoopConfiguration.set("fs.defaultFS", properties.getProperty("fs.defaultFS"))
    sc.hadoopConfiguration.set("dfs.nameservices", properties.getProperty("dfs.nameservices"))
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", properties.getProperty("dfs.ha.namenodes.nns"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", properties.getProperty("dfs.namenode.rpc-address.nns.nn1"))
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", properties.getProperty("dfs.namenode.rpc-address.nns.nn2"))
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", properties.getProperty("dfs.client.failover.proxy.provider.nns"))

    // 时间戳转换日期
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    getRequestTypeRatio(spark, sc, df, task)
    getResponseCodeRatio(spark, sc, df, task)
    getResponseTypeRatio(spark, sc, df, task)
    getQpsRatio(spark, sc, df, task)
    getResponseCodeDomainRatio(spark, sc, df, task)
    getResponseCodeClientIpRatio(spark, sc, df, task)

    task match {
      case "hh" => getNATAnalyze(sc)
      case "dd" => getNATAnalyzeDD(sc)
      case _ => println("NAT日志分析跳过执行...")
    }

  }

  private def getNATAnalyze(sc: SparkContext): Unit = {

    case class EsNatAnalyze(accesstime: Long, types: String, data: String, count: Long, key: String)

    val query1 = s"""{\"query\":{\"bool\":{\"must\":[{\"term\":{ \"accesstime\": \"${DNSUtils.getTaskTime._3._1 * 1000L}\"}}]}}}"""

    val maps: Map[String, Long] = sc.esRDD(s"bigdata_nat_report", query1)
      .values
      .map(per => {
        val key = per.getOrElse("key", "unknown").toString
        val value = per.getOrElse("count", 0).toString.toDouble.toLong
        (key, value)
      })
      .collect.toMap[String, Long]

    val broadMaps: Broadcast[Map[String, Long]] = sc.broadcast(maps)

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(new Date().getTime)
    val lower = DNSUtils.getTaskTime._1._2
    val upper = DNSUtils.getTaskTime._1._1
    val query2 = s"""{\"query\":{\"bool\":{\"must\":[{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
    val valueRdd: RDD[EsNatAnalyze] = sc.esRDD(s"bigdata_nat_flow_${date}", query2)
      .values
      .map(per => {
        //        val date: Long = dateFormat.parse(yesterday).getTime / 1000
        val date: Long = DNSUtils.getTaskTime._3._1
        val data: String = per.getOrElse("data", "").toString
        val count = per.getOrElse("count", "0").toString.toDouble.toLong
        val types: String = per.getOrElse("types", "").toString
        val key = date + "_" + types + "_" + data
        (key, count)
      })
      .distinct()
      .map(per => (per._1, per._2))
      .reduceByKey(_ + _)
      .map(per => {
        val mapsValue: Map[String, Long] = broadMaps.value
        val accesstime: Long = per._1.split("_")(0).toLong * 1000
        val types: String = per._1.split("_")(1)
        val data: String = per._1.split("_")(2)
        if (mapsValue.nonEmpty && mapsValue.contains(per._1)) {
          EsNatAnalyze(accesstime, types, data, per._2 + mapsValue(per._1), per._1)
        } else {
          EsNatAnalyze(accesstime, types, data, per._2, per._1)
        }
      })

    EsSpark.saveToEs(valueRdd,"bigdata_nat_report/nat",Map("es.mapping.id" -> "key"))

  }

  private def getNATAnalyzeDD(sc: SparkContext): Unit = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val date: String = dateFormat.format(System.currentTimeMillis())
    val lower = DNSUtils.getTaskTime._3._2

    val destinationIp = sc.textFile(s"hdfs://nns/NATIp/$date*/*/part*")
      .map(perline => {
        val strings: Array[String] = perline.split("[,()]")
        val accesstime: Long = dateFormat.parse(dateFormat.format(strings(1).toLong * 1000)).getTime
        val data: String = strings(2)
        val count = strings(3).toLong
        (("destinationIp", accesstime, data), count)
      })
      .filter(_._1._2 == lower * 1000L)
      .reduceByKey(_ + _)
      .map(per => Map("accesstime" -> per._1._2, "types" -> per._1._1, "data" -> per._1._3, "count" -> per._2))

    EsSpark.saveToEs(destinationIp, "bigdata_nat_report/nat")
  }


  /**
   * 请求类型维度数据
   */
  private def getRequestTypeRatio(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"requestType\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var requestType: String = per.getOrElse("requestType", "").toString
          if (requestType.equals("None"))
            requestType = requestType.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, requestType)
        })
        .toDF("num", "accessTime", "clientName", "requestType")
        .createOrReplaceTempView("requestType_avg")

      getRequestType2ES(spark, df, "requestType_avg", "avg", "bigdata_dns_flow_ratio_hh/ratio")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"requestType\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_hh", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var requestType: String = per.getOrElse("requestType", "").toString
          if (requestType.equals("None"))
            requestType = requestType.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, requestType)
        })
        .toDF("num", "accessTime", "clientName", "requestType")
        .createOrReplaceTempView("requestType_avg_6h")

      getRequestType2ES(spark, df, "requestType_avg_6h", "avg", "bigdata_dns_flow_ratio_6h/ratio")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"requestType\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var requestType: String = per.getOrElse("requestType", "").toString
          if (requestType.equals("None"))
            requestType = requestType.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, requestType)
        })
        .toDF("num", "accessTime", "clientName", "requestType")
        .createOrReplaceTempView("requestType_avg_dd")

      getRequestType2ES(spark, df, "requestType_avg_dd", "sum", "bigdata_dns_flow_ratio_dd/ratio")

    }
  }

  /**
   * 请求类型维度数据聚合
   */
  def getRequestType2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[RatioBeanRequestType] =
      spark.sql(
        s"""
           |select requestType,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by requestType, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val requestType: String = line.getAs[String]("requestType")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          RatioBeanRequestType("requestType", number, requestType, clientName.toInt, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * 响应代码维度
   */
  private def getResponseCodeRatio(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseCode\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var responseCode: String = per.getOrElse("responseCode", "").toString
          if (responseCode.equals("None"))
            responseCode = responseCode.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, responseCode)
        })
        .toDF("num", "accessTime", "clientName", "responseCode")
        .createOrReplaceTempView("responseCode_avg")

      getResponseCode2ES(spark, df, "responseCode_avg", "avg", "bigdata_dns_flow_ratio_hh/ratio")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseCode\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_hh", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var responseCode: String = per.getOrElse("responseCode", "").toString
          if (responseCode.equals("None"))
            responseCode = responseCode.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, responseCode)
        })
        .toDF("num", "accessTime", "clientName", "responseCode")
        .createOrReplaceTempView("responseCode_avg_6h")

      getResponseCode2ES(spark, df, "responseCode_avg_6h", "avg", "bigdata_dns_flow_ratio_6h/ratio")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseCode\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var responseCode: String = per.getOrElse("responseCode", "").toString
          if (responseCode.equals("None"))
            responseCode = responseCode.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, responseCode)
        })
        .toDF("num", "accessTime", "clientName", "responseCode")
        .createOrReplaceTempView("responseCode_avg_dd")

      getResponseCode2ES(spark, df, "responseCode_avg_dd", "sum", "bigdata_dns_flow_ratio_dd/ratio")

    }
  }

  /**
   * 响应代码维度数据聚合
   */
  def getResponseCode2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[RatioBeanResponseCode] =
      spark.sql(
        s"""
           |select responseCode,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by responseCode, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val responseCode: String = line.getAs[String]("responseCode")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          RatioBeanResponseCode("responseCode", number, responseCode, clientName.toInt, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * 响应类型维度
   */
  private def getResponseTypeRatio(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseType\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var responseType: String = per.getOrElse("responseType", "").toString
          if (responseType.equals("None"))
            responseType = responseType.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, responseType)
        })
        .toDF("num", "accessTime", "clientName", "responseType")
        .createOrReplaceTempView("responseType_avg")

      getResponseType2ES(spark, df, "responseType_avg", "avg", "bigdata_dns_flow_ratio_hh/ratio")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseType\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_hh", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var responseType: String = per.getOrElse("responseType", "").toString
          if (responseType.equals("None"))
            responseType = responseType.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, responseType)
        })
        .toDF("num", "accessTime", "clientName", "responseType")
        .createOrReplaceTempView("responseType_avg_6h")

      getResponseType2ES(spark, df, "responseType_avg_6h", "avg", "bigdata_dns_flow_ratio_6h/ratio")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseType\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var responseType: String = per.getOrElse("responseType", "").toString
          if (responseType.equals("None"))
            responseType = responseType.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, responseType)
        })
        .toDF("num", "accessTime", "clientName", "responseType")
        .createOrReplaceTempView("responseCode_avg_dd")

      getResponseType2ES(spark, df, "responseCode_avg_dd", "sum", "bigdata_dns_flow_ratio_dd/ratio")

    }
  }

  /**
   * 响应类型维度数据聚合
   */
  def getResponseType2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[RatioBeanResponseType] =
      spark.sql(
        s"""
           |select responseType,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by responseType, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val responseType: String = line.getAs[String]("responseType")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          RatioBeanResponseType("responseType", number, responseType, clientName.toInt, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * qps解析数维度
   */
  private def getQpsRatio(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"qps\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val error: String = per.getOrElse("error", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var avgNum: Long = per.getOrElse("avgNum", "").toString.toLong
          (num, error, accessTime, clientName, avgNum)
        })
        .toDF("num", "error", "accessTime", "clientName", "avgNum")
        .createOrReplaceTempView("qps_avg")

      getQps2ES(spark, df, "qps_avg", "avg", "bigdata_dns_flow_ratio_hh/ratio")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"qps\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_hh", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val error: String = per.getOrElse("error", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var avgNum: String = per.getOrElse("avgNum", "").toString
          if (avgNum.equals("None"))
            avgNum = avgNum.replaceAll("\\S{4}", "")
          (num, error, accessTime, clientName, avgNum)
        })
        .toDF("num", "error", "accessTime", "clientName", "avgNum")
        .createOrReplaceTempView("qps_avg_6h")

      getQps2ES(spark, df, "qps_avg_6h", "avg", "bigdata_dns_flow_ratio_6h/ratio")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"qps\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_v1", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val error: String = per.getOrElse("error", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var avgNum: String = per.getOrElse("avgNum", "").toString
          if (avgNum.equals("None"))
            avgNum = avgNum.replaceAll("\\S{4}", "")
          (num, error, accessTime, clientName, avgNum)
        })
        .toDF("num", "error", "accessTime", "clientName", "avgNum")
        .createOrReplaceTempView("qps_avg_dd")

      getQps2ES(spark, df, "qps_avg_dd", "sum", "bigdata_dns_flow_ratio_dd/ratio")

    }
  }

  /**
   * qps解析数数据聚合
   */
  def getQps2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[RatioBeanQps] =
      spark.sql(
        s"""
           |select clientName,accessTime,ceil(${function}(num)) number,ceil(${function}(error)) error,floor(${function}(num)/300) avgNum
           |from
           |${viewName}
           |group by clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val avgNum: Long = line.getAs[Long]("avgNum")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          val error: Long = line.getAs[Long]("error")
          RatioBeanQps("qps", number, error, avgNum, clientName.toInt, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * 响应代码关联域名维度
   */
  private def getResponseCodeDomainRatio(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseCodeDomain\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      val month = new SimpleDateFormat("yyyyMM").format(lower * 1000)
      sc.esRDD(s"bigdata_dns_flow_ratio_${month}", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var domain: String = per.getOrElse("domain", "").toString
          var responseCode: String = per.getOrElse("responseCode", "").toString
          if (responseCode.equals("None"))
            responseCode = responseCode.replaceAll("\\S{4}", "")
          if (domain.equals("None"))
            domain = domain.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, domain, responseCode)
        })
        .toDF("num", "accessTime", "clientName", "domain", "responseCode")
        .createOrReplaceTempView("responseCode_avg")

      getResponseCodeDomain2ES(spark, df, "responseCode_avg", "avg", "bigdata_dns_flow_ratio_hh/ratio")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseCodeDomain\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_hh", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var domain: String = per.getOrElse("domain", "").toString
          var responseCode: String = per.getOrElse("responseCode", "").toString
          if (responseCode.equals("None"))
            responseCode = responseCode.replaceAll("\\S{4}", "")
          if (domain.equals("None"))
            domain = domain.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, domain, responseCode)
        })
        .toDF("num", "accessTime", "clientName", "domain", "responseCode")
        .createOrReplaceTempView("responseCode_avg_6h")

      getResponseCodeDomain2ES(spark, df, "responseCode_avg_6h", "avg", "bigdata_dns_flow_ratio_6h/ratio")

    } else if ("dd".equals(task)) {

    }
  }

  /**
   * 响应代码关联域名维度数据聚合
   */
  def getResponseCodeDomain2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[RatioBeanResponseCodeDomain] =
      spark.sql(
        s"""
           |select responseCode,domain,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by responseCode, domain, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val responseCode: String = line.getAs[String]("responseCode")
          val clientName: String = line.getAs[String]("clientName")
          val domain: String = line.getAs[String]("domain")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          RatioBeanResponseCodeDomain("responseCodeDomain", number, domain, clientName.toInt, responseCode, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * 响应代码关联源ip维度
   */
  private def getResponseCodeClientIpRatio(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseCodeClientIP\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      val month = new SimpleDateFormat("yyyyMM").format(lower * 1000)
      sc.esRDD(s"bigdata_dns_flow_ratio_${month}", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var clientIp: String = per.getOrElse("clientIp", "").toString
          var responseCode: String = per.getOrElse("responseCode", "").toString
          if (responseCode.equals("None"))
            responseCode = responseCode.replaceAll("\\S{4}", "")
          if (clientIp.equals("None"))
            clientIp = clientIp.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, clientIp, responseCode)
        })
        .toDF("num", "accessTime", "clientName", "clientIp", "responseCode")
        .createOrReplaceTempView("responseCode_avg")

      getResponseCodeClientIp2ES(spark, df, "responseCode_avg", "avg", "bigdata_dns_flow_ratio_hh/ratio")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"responseCodeClientIP\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_ratio_hh", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var clientIp: String = per.getOrElse("clientIp", "").toString
          var responseCode: String = per.getOrElse("responseCode", "").toString
          if (responseCode.equals("None"))
            responseCode = responseCode.replaceAll("\\S{4}", "")
          if (clientIp.equals("None"))
            clientIp = clientIp.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, clientIp, responseCode)
        })
        .toDF("num", "accessTime", "clientName", "clientIp", "responseCode")
        .createOrReplaceTempView("responseCode_avg_6h")

      getResponseCodeClientIp2ES(spark, df, "responseCode_avg_6h", "avg", "bigdata_dns_flow_ratio_6h/ratio")

    } else if ("dd".equals(task)) {

    }
  }

  /**
   * 响应代码关联源ip维度数据聚合
   */
  def getResponseCodeClientIp2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[RatioBeanResponseCodeClientIp] =
      spark.sql(
        s"""
           |select responseCode,clientIp,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by responseCode, clientIp, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val responseCode: String = line.getAs[String]("responseCode")
          val clientName: String = line.getAs[String]("clientName")
          val clientIp: String = line.getAs[String]("clientIp")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          RatioBeanResponseCodeClientIp("responseCodeClientIP", number, clientIp, clientName.toInt, responseCode, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }
}
