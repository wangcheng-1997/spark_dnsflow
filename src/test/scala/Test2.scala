import com.alibaba.fastjson.JSON
import redis.clients.jedis.Jedis

/**
  * ClassName Test2
  * Date 2020/1/2 20:14
  **/
object Test2 {

  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("30.250.11.158", 6399, 6000)

    jedis.hgetAll(jedis.hget("ONLINEUSERS:CONSTANTS", "ID_OBJECT"))
      .values().toArray()
      .map(json => {
        val jobj = JSON.parseObject(json.toString)
        (jobj.getString("ip"), jobj.getString("user"))
      })

    val data = jedis.hget(jedis.hget("ONLINEUSERS:CONSTANTS", "ID_OBJECT"), jedis.hget(jedis.hget("ONLINEUSERS:CONSTANTS", "IP_ID"), "100.104.18.47"))

    println(data)
  }
}
