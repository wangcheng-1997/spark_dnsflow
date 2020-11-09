import com.zjtuojing.dnsflow.BeanObj._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * ClassName Test4
  * Date 2020/1/10 10:28
  **/
object Test4 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setAppName("SparkStreamingReadHDFS")
    conf.setMaster("local[4]")
//    conf.set("fs.defaultFS", "hdfs://nameservice1")
//    conf.set("dfs.nameservices", "nameservice1")
//    conf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2")
//    conf.set("dfs.namenode.rpc-address.nameservice1.nn1", "30.250.60.2:8020")
//    conf.set("dfs.namenode.rpc-address.nameservice1.nn2", "30.250.60.7:8020")
//    conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    val ssc = new StreamingContext(conf, Seconds(5))
    val sc = ssc.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nameservice1", "nn1,nn2")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.nn1", "30.250.60.2:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.nn2", "30.250.60.7:8020")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")





    ssc.start()
    ssc.awaitTermination()

  }

}
