package dzg.scala.day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 根据访问日志，计算最受欢迎的老师
 *
 * @author BelieverDzg
 * @date 2020/10/26 19:43
 */
object FavouriteTeacher {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("FavouriteTeacher").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    //读取数据
    val lines: RDD[String] = sc.textFile(args(0))

    val teacherAndOne: RDD[(String, Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      //      val url: String = line.substring(0,index)
      //      val subject: String = new URL(url).getHost.split("[.]")(0)
      (teacher, 1)
    })
    val reducedTeacher: RDD[(String, Int)] = teacherAndOne.reduceByKey(_+_)

    val sorted: RDD[(String, Int)] = reducedTeacher.sortBy(_._2,false)

    //出发action
    val tuples: Array[(String, Int)] = sorted.collect().take(3)

    println(tuples.toBuffer)

    sc.stop()
  }
}
