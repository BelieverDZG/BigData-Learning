package dzg.scala.day5

import java.sql
import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/27 10:56
 */
object JdbcRddDemo {

  def main(args: Array[String]): Unit = {

    val getConn = () => {
      //数据库链接
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?,.....");
    }
    
    val conf: SparkConf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，从MySQL中读取数据
    //new了RDD，里面没有真正要计算的数据
    val jdbcRDD = new JdbcRDD(
      sc,
      getConn,
      "SELECT * FROM logs where id >= ? and id < ?",
      1,
      5,
      2,//分区数量
      line => {
        val ID: Int = line.getInt(0)
        val name: String = line.getString(1)
        val age: Int = line.getInt(2)
        (ID,name,age)
      }
    )

    //触发action
    val tuples: Array[(Int, String, Int)] = jdbcRDD.collect()


  }


}
