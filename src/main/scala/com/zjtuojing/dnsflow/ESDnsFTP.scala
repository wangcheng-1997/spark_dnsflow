package com.zjtuojing.dnsflow

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.zjtuojing.dnsflow.BeanObj._
import com.zjtuojing.utils.DNSUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

/**
 * ClassName ESDnsFTP
 * Date 2020/6/2 17:28
 */
object ESDnsFTP {

  val properties = DNSUtils.loadConf()

  def main(args: Array[String]): Unit = {

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
        classOf[FTPBeanProvince],
        classOf[FTPBeanOperator],
        classOf[FTPBeanDnsIp],
        classOf[FTPBeanBusiness],
        classOf[FTPBeanUser]
      )
    )
    //      .set("mapping.date.rich", "false")
    //      .set("max.docs.per.partition","100")
    //      .set("spark.es.scroll.size","10000")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    // 时间戳转换日期
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    getProvinceFTP(spark, sc, df, task)
    getOperatorFTP(spark, sc, df, task)
    getDnsIPFTP(spark, sc, df, task)
    getBusinessFTP(spark, sc, df, task)
    getUserFTP(spark, sc, df, task)

    sc.stop()

  }

  /**
   * 省份维度数据
   */
  private def getProvinceFTP(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"province\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var province: String = per.getOrElse("province", "").toString
          if (province.equals("None"))
            province = province.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, province)
        })
        .toDF("num", "accessTime", "clientName", "province")
        .createOrReplaceTempView("province_avg")

      getProvince2ES(spark, df, "province_avg", "avg", "bigdata_dns_flow_top_hh_2020/aip")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"province\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top_hh_2020", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var province: String = per.getOrElse("province", "").toString
          if (province.equals("None"))
            province = province.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, province)
        })
        .toDF("num", "accessTime", "clientName", "province")
        .createOrReplaceTempView("province_avg_6h")

      getProvince2ES(spark, df, "province_avg_6h", "avg", "bigdata_dns_flow_top_6h/aip")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"province\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var province: String = per.getOrElse("province", "").toString
          if (province.equals("None"))
            province = province.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, province)
        })
        .toDF("num", "accessTime", "clientName", "province")
        .createOrReplaceTempView("province_avg_dd")

      getProvince2ES(spark, df, "province_avg_dd", "sum", "bigdata_dns_flow_top_dd/aip")

    }
  }

  /**
   * 省份维度数据聚合
   */
  def getProvince2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[FTPBeanProvince] =
      spark.sql(
        s"""
           |select province,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by province, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val province: String = line.getAs[String]("province")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          FTPBeanProvince("province", number, province, clientName.toInt, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * 运营商维度
   */
  private def getOperatorFTP(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"operator\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var operator: String = per.getOrElse("operator", "").toString
          if (operator.equals("None"))
            operator = operator.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, operator)
        })
        .toDF("num", "accessTime", "clientName", "operator")
        .createOrReplaceTempView("operator_avg")

      getOperator2ES(spark, df, "operator_avg", "avg", "bigdata_dns_flow_top_hh_2020/aip")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"operator\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top_hh_2020", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var operator: String = per.getOrElse("operator", "").toString
          if (operator.equals("None"))
            operator = operator.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, operator)
        })
        .toDF("num", "accessTime", "clientName", "operator")
        .createOrReplaceTempView("operator_avg_6h")

      getOperator2ES(spark, df, "operator_avg_6h", "avg", "bigdata_dns_flow_top_6h/aip")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"operator\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var operator: String = per.getOrElse("operator", "").toString
          if (operator.equals("None"))
            operator = operator.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, operator)
        })
        .toDF("num", "accessTime", "clientName", "operator")
        .createOrReplaceTempView("operator_avg_dd")

      getOperator2ES(spark, df, "operator_avg_dd", "sum", "bigdata_dns_flow_top_dd/aip")

    }
  }

  /**
   * 运营商维度数据聚合
   */
  def getOperator2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[FTPBeanOperator] =
      spark.sql(
        s"""
           |select operator,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by operator, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val operator: String = line.getAs[String]("operator")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          FTPBeanOperator("operator", number, operator, clientName.toInt, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * 资源名称维度
   */
  private def getBusinessFTP(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"business\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var resourceName: String = per.getOrElse("resourceName", "").toString
          var resourceType: String = per.getOrElse("resourceType", "").toString
          val resourceProps: String = per.get("resourceProps").get.asInstanceOf[String]
          if (resourceName.equals("None"))
            resourceName = resourceName.replaceAll("\\S{4}", "")
          if (resourceType.equals("None"))
            resourceType = resourceType.replaceAll("\\S{4}", "")
          (num, accessTime, resourceName, resourceType, resourceProps, clientName)
        })
        .toDF("num", "accessTime", "resourceName", "resourceType", "resourceProps", "clientName")
        .createOrReplaceTempView("business_avg")

      getBusiness2ES(spark, df, "business_avg", "avg", "bigdata_dns_flow_top_hh_2020/aip")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"business\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top_hh_2020", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var resourceName: String = per.getOrElse("resourceName", "").toString
          var resourceType: String = per.getOrElse("resourceType", "").toString
          val resourceProps: String = per.get("resourceProps").get.asInstanceOf[String]
          if (resourceName.equals("None"))
            resourceName = resourceName.replaceAll("\\S{4}", "")
          if (resourceType.equals("None"))
            resourceType = resourceType.replaceAll("\\S{4}", "")
          (num, accessTime, resourceName, resourceType, resourceProps, clientName)
        })
        .toDF("num", "accessTime", "resourceName", "resourceType", "resourceProps", "clientName")
        .createOrReplaceTempView("business_avg_6h")

      getBusiness2ES(spark, df, "business_avg_6h", "avg", "bigdata_dns_flow_top_6h/aip")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"business\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var resourceName: String = per.getOrElse("resourceName", "").toString
          var resourceType: String = per.getOrElse("resourceType", "").toString
          val resourceProps: String = per.get("resourceProps").get.asInstanceOf[String]
          if (resourceName.equals("None"))
            resourceName = resourceName.replaceAll("\\S{4}", "")
          if (resourceType.equals("None"))
            resourceType = resourceType.replaceAll("\\S{4}", "")
          (num, accessTime, resourceName, resourceType, resourceProps, clientName)
        })
        .toDF("num", "accessTime", "resourceName", "resourceType", "resourceProps", "clientName")
        .createOrReplaceTempView("business_avg_dd")

      getBusiness2ES(spark, df, "business_avg_dd", "sum", "bigdata_dns_flow_top_dd/aip")

    }
  }

  /**
   * 资源名称维度数据聚合
   */
  def getBusiness2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[FTPBeanBusiness] =
      spark.sql(
        s"""
           |select resourceName,resourceProps,resourceType,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by clientName,resourceName,resourceProps,accessTime,resourceType
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val resourceName: String = line.getAs[String]("resourceName")
          val resourceType: String = line.getAs[String]("resourceType")
          val resourceProps: String = line.getAs[String]("resourceProps")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          FTPBeanBusiness("business", number, clientName.toInt, resourceProps, resourceName, resourceType, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * DNS服务器ip维度
   */
  private def getDnsIPFTP(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"dnsIp\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var dnsIp: String = per.getOrElse("dnsIp", "").toString
          if (dnsIp.equals("None"))
            dnsIp = dnsIp.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, dnsIp)
        })
        .toDF("num", "accessTime", "clientName", "dnsIp")
        .createOrReplaceTempView("dnsIp_avg")

      getDnsIP2ES(spark, df, "dnsIp_avg", "avg", "bigdata_dns_flow_top_hh_2020/aip")

    } else if ("6h".equals(task)) {
      // 6小时任务6h

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._2._1
      val lower = DNSUtils.getTaskTime._2._2

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"dnsIp\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top_hh_2020", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val time: String = df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000)
          val hour: Int = time.substring(11, 13).toInt
          val accessTime: String = time.replaceAll("\\s+\\d{2}", s" ${hour / 6 * 6}")
          val clientName: String = per.getOrElse("clientName", "").toString
          var dnsIp: String = per.getOrElse("dnsIp", "").toString
          if (dnsIp.equals("None"))
            dnsIp = dnsIp.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, dnsIp)
        })
        .toDF("num", "accessTime", "clientName", "dnsIp")
        .createOrReplaceTempView("dnsIp_avg_6h")

      getDnsIP2ES(spark, df, "dnsIp_avg_6h", "avg", "bigdata_dns_flow_top_6h/aip")

    } else if ("dd".equals(task)) {
      // 按天任务dd

      // 获取时间范围
      val upper = DNSUtils.getTaskTime._3._1
      val lower = DNSUtils.getTaskTime._3._2

      // 查询语句
      val query: String = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"dnsIp\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""

      sc.esRDD("bigdata_dns_flow_top", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 10).concat(" 00:00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var dnsIp: String = per.getOrElse("dnsIp", "").toString
          if (dnsIp.equals("None"))
            dnsIp = dnsIp.replaceAll("\\S{4}", "")
          (num, accessTime, clientName, dnsIp)
        })
        .toDF("num", "accessTime", "clientName", "dnsIp")
        .createOrReplaceTempView("dnsIp_avg_dd")

      getDnsIP2ES(spark, df, "dnsIp_avg_dd", "sum", "bigdata_dns_flow_top_dd/aip")

    }
  }

  /**
   * DNS服务器ip维度数据聚合
   */
  def getDnsIP2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[FTPBeanDnsIp] =
      spark.sql(
        s"""
           |select dnsIp,clientName,accessTime,ceil(${function}(num)) number
           |from
           |${viewName}
           |group by dnsIp, clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val dnsIp: String = line.getAs[String]("dnsIp")
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          FTPBeanDnsIp("dnsIp", number, dnsIp, clientName.toInt, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

  /**
   * 用户维度
   */
  private def getUserFTP(spark: SparkSession, sc: SparkContext, df: SimpleDateFormat, task: String): Unit = {

    import spark.implicits._

    if ("hh".equals(task)) {
      // 小时任务hh

      // 获取时间范围
      val upper: Long = DNSUtils.getTaskTime._1._1
      val lower: Long = DNSUtils.getTaskTime._1._2
      val year = df.format(lower*1000).substring(0,4)

      // 查询语句
      val query = s"""{\"query\":{\"bool\":{\"must\":[{\"match\":{\"types\":\"user\"}},{\"range\":{ \"accesstime\": { \"gte\": \"$lower\",\"lt\": \"$upper\"}}}]}}}"""
      sc.esRDD(s"bigdata_dns_flow_top_user_$year", query)
        .values
        .map(per => {
          val num: String = per.getOrElse("number", "").toString
          val inNet: String = per.getOrElse("inNet", "").toString
          val error: String = per.getOrElse("error", "").toString
          val accessTime: String =
            df.format(per.get("accesstime").get.asInstanceOf[Date].getTime * 1000).substring(0, 13).concat(":00:00")
          val clientName: String = per.getOrElse("clientName", "").toString
          var address: String = per.getOrElse("address", "").toString
          var phone: String = per.getOrElse("phone", "").toString
          var domain: String = per.getOrElse("domain", "").toString
          var clientIp: String = per.getOrElse("clientIp", "").toString
          var userName: String = per.getOrElse("userName", "").toString
          if (address.equals("None"))
            address = address.replaceAll("\\S{4}", "")
          if (phone.equals("None"))
            phone = phone.replaceAll("\\S{4}", "")
          if (domain.equals("None"))
            domain = domain.replaceAll("\\S{4}", "")
          if (clientIp.equals("None"))
            clientIp = clientIp.replaceAll("\\S{4}", "")
          if (userName.equals("None"))
            userName = userName.replaceAll("\\S{4}", "")
          (num, inNet, error, accessTime, clientName, address, phone, domain, clientIp, userName)
        })
        .toDF("num", "inNet", "error", "accessTime", "clientName", "address", "phone", "domain", "clientIp", "userName")
        .createOrReplaceTempView("user_avg")

      getUser2ES(spark, df, "user_avg", "avg", "bigdata_dns_flow_top_user_hh/aip")

    }
  }

  /**
   * 用户维度数据聚合
   */
  def getUser2ES(spark: SparkSession, df: SimpleDateFormat, viewName: String, function: String, resource: String): Unit = {
    val avgRDD: RDD[FTPBeanUser] =
      spark.sql(
        s"""
           |select clientName,accessTime,ceil(${function}(num)) number,ceil(${function}(inNet)) inNet,ceil(${function}(error)) error,
           |address,phone,domain,clientIp,userName
           |from
           |${viewName}
           |group by userName,clientIp,domain,phone,address,clientName, accessTime
           |""".stripMargin)
        .rdd
        .map((line: Row) => {
          val clientName: String = line.getAs[String]("clientName")
          val accessTime: Long = df.parse(line.getAs("accessTime")).getTime / 1000
          val number: Long = line.getAs[Long]("number")
          val inNet: Long = line.getAs[Long]("inNet")
          val error: Long = line.getAs[Long]("error")
          val address: String = line.getAs[String]("address")
          val phone: String = line.getAs[String]("phone")
          val domain: String = line.getAs[String]("domain")
          val clientIp: String = line.getAs[String]("clientIp")
          val userName: String = line.getAs[String]("userName")
          FTPBeanUser("user", number, inNet, address, clientName.toInt, phone, domain, clientIp, error, userName, accessTime)
        })
    EsSpark.saveToEs(avgRDD, resource)
  }

}
