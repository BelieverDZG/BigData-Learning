package dzg.java.prost

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * @author BelieverDzg
 * @date 2020/12/25 14:53
 */
object BroadCastJoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("BroadCastJoin")
    val sc = new SparkContext(conf)

    /**
     * map-side-join
     * 取出小表中出现的用户与大表关联后取出所需要的信息
     **/
    //部分人信息(身份证,姓名)
    val people_info = sc.parallelize(Array(("110", "lsw"), ("222", "yyy"))).collectAsMap()
    //全国的学生详细信息(身份证,学校名称,学号...)
    val student_all = sc.parallelize(Array(("110", "s1", "211"),
      ("111", "s2", "222"),
      ("112", "s3", "233"),
      ("113", "s2", "244")))

    //将需要关联的小表进行关联
    val people_bc = sc.broadcast(people_info)

    /**
     * 使用mapPartition而不是用map，减少创建broadCastMap.value的空间消耗
     * 同时匹配不到的数据也不需要返回（）
     **/
    val res = student_all.mapPartitions(iter => {
      val stuMap = people_bc.value
      val arrayBuffer = ArrayBuffer[(String, String, String)]()
      iter.foreach { case (idCard, school, sno) => {
        if (stuMap.contains(idCard)) {
          arrayBuffer.+=((idCard, stuMap.getOrElse(idCard, ""), school))
        }
      }
      }
      arrayBuffer.iterator
    })

    /**
     * 使用另一种方式实现
     * 使用for的守卫
     **/
    /*val res1 = student_all.mapPartitions(iter => {
      val stuMap = people_bc.value
      for {
        (idCard, school, sno) <- iter
        if (stuMap.contains(idCard))
      } yield (idCard, stuMap.getOrElse(idCard, ""), school)
    })*/

    res.foreach(println)

    //http://dmtolpeko.com/2015/02/20/map-side-join-in-spark/
    // Fact table
    val flights = sc.parallelize(List(
      ("SEA", "JFK", "DL", "418",  "7:00"),
      ("SFO", "LAX", "AA", "1250", "7:05"),
      ("SFO", "JFK", "VX", "12",   "7:05"),
      ("JFK", "LAX", "DL", "424",  "7:10"),
      ("LAX", "SEA", "DL", "5737", "7:10")))

    // Dimension table
    val airports = sc.parallelize(List(
      ("JFK", "John F. Kennedy International Airport", "New York", "NY"),
      ("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
      ("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
      ("SFO", "San Francisco International Airport", "San Francisco", "CA")))

    // Dimension table
    val airlines = sc.parallelize(List(
      ("AA", "American Airlines"),
      ("DL", "Delta Airlines"),
      ("VX", "Virgin America")))

    val airportsMap = sc.broadcast(airports.map{case(a, b, c, d) => (a, c)}.collectAsMap)
    val airlinesMap = sc.broadcast(airlines.collectAsMap)

    flights.map{case(a, b, c, d, e) =>
      (airportsMap.value.get(a).get,
        airportsMap.value.get(b).get,
        airlinesMap.value.get(c).get, d, e)}.collect
    sc.stop()
  }


}
