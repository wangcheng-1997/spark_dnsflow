import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ClassName Test5
  * Date 2020/1/10 11:34
  **/
object Test5 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("app")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nns")
    sc.hadoopConfiguration.set("dfs.nameservices", "nns")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nns", "nn1,nn2")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn1", "30.250.60.2:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nns.nn2", "30.250.60.7:8020")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    println(sc.textFile("hdfs://spark/Redis/20190907").first())


    sc.stop()

  }

}
