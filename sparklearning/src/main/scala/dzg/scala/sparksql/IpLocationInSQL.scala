package dzg.scala.sparksql

import dzg.scala.day4.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author BelieverDzg
 * @date 2020/10/30 14:15
 */
object IpLocationInSQL {
  /**
   * jon的代价太昂贵，而且非常慢，解决思路是将表缓存起来（广播变量）
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("IpLocationInSQL")
      .master("local[*]")
      .getOrCreate()
    //读取规则
    val rules: Dataset[String] = spark.read.textFile(args(0))

    //整理ip规则数据
    import spark.implicits._
    val ruleDataSet: Dataset[(Long, Long, String)] = rules.map(line => {
      val array: Array[String] = line.split("[|]")
      val startNum: Long = array(2).toLong
      val endNum: Long = array(3).toLong
      val province: String = array(6)
      (startNum, endNum, province)
    })
    //收集ip规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ruleDataSet.collect()
    //使用sparkContext进行广播，将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)

    //读取访问日志
    val logs: Dataset[String] = spark.read.textFile(args(1))

    //整理日志数据
    val ipDataFrame: DataFrame = logs.map(log => {
      val array: Array[String] = log.split("[|]")
      val ip: String = array(1)
      val ipLongValue: Long = MyUtils.ip2Long(ip)
      ipLongValue
    }).toDF("ip_val")
    ipDataFrame.createTempView("v_log")

    //自定义一个函数（UDF），并注册
    val ip2Province: UserDefinedFunction = spark.udf.register("ip2Province", (ipNum: Long) => {
      val rules: Array[(Long, Long, String)] = broadcastRef.value
      val index: Int = MyUtils.binarySearch(rules, ipNum)
      var province = "不知道"
      if (index != -1) {
        province = rules(index)._3
      }
      province
    })

    val res: DataFrame = spark.sql("select ip2Province(ip_num) province,count(*) counts from " +
      "v_log group by province order by counts desc")
    res.show()

    spark.stop()
  }
}

class IpLocation {

  def ipLocation(rulesPath: String, logsPath: String) = {
    val spark = SparkSession
      .builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    //取到HDFS中的ip规则
    import spark.implicits._
    val rulesLines: Dataset[String] = spark.read.textFile(rulesPath)
    //整理ip规则数据()
    val ruleDataFrame: DataFrame = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")


    //创建RDD，读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(logsPath)

    //整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      //将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    //注册两个视图，然后基于视图进行sql查询操作
    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    val queryResult: DataFrame = spark.sql("select province , count(*) count from v_ips join v_rules" +
      "on (ip_num >= snum ans ip_num <= enum) " +
      "group by province" +
      "order by counts desc")
    queryResult.show()
    spark.stop()

  }
}
