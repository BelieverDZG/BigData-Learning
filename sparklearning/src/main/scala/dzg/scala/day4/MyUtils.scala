package dzg.scala.day4

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

/**
 * @author BelieverDzg
 * @date 2020/10/27 15:43
 *
 */
object MyUtils {

  /**
   * 将ip地址转换为一个整数表示
   *
   * @param ip
   * @return
   */
  def ip2Long(ip: String): Long = {
    var fragments = ip.split("[.]")
    var ans = 0L
    for (i <- 0 until fragments.length) {
      ans = fragments(i).toLong | ans << 8L
    }
    ans
  }

  /**
   * 从文件中读取ip地址与对应的地域之间的关联关系
   *
   * @param path
   * @return
   */
  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val low: Long = fields(2).toLong
      val high: Long = fields(3).toLong
      val province: String = fields(6)
      (low, high, province)
    }).toArray
    rules
  }

  /**
   * 访问规则是有序的，使用二分查找 ip地址对应的位置
   *
   * @param lines
   * @param ip
   * @return
   */
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  /**
   * 将数据写入MySQL中
   *
   * @param it
   */
  def data2MySQL(it: Iterator[(String, Int)]): Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //先获得一个JDBC连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=true",
      "root",
      "root")
    val pstm: PreparedStatement = conn.prepareStatement("insert  into logs values (?,?)")

    //将分区中的主句一个一个写入数据库
    it.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    //将分区数据全部写完之后，关闭连接
    if (pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

}
