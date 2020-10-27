package dzg.scala.day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @Date 2020/10/27 20:07
 *
 *  求不同学科最受欢迎的老师
 */
object GroupFavTeacherTwo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacherTwo").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    val subjects = Array("bigdata","javaee","php")

    val lines: RDD[String] = sc.textFile(args(0))

    val groupAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val url: String = line.substring(0, index)
      val subject: String = new URL(url).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    //使用（学科,老师）作为key
    val reduced: RDD[((String, String), Int)] = groupAndOne.reduceByKey(_+_)

//    println(reduced.collect().toBuffer)
    //cache到内存
    //val cached = reduced.cache()

    //scala的集合排序是在内存中进行的，但是内存有可能不够用
    //可以调用RDD的sortby方法，内存+磁盘进行排序

    for(sb <- subjects){
      //该RDD中对应的数据仅有一个学科的数据，因为过滤了
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)

      //调用RDD的sortBy方法，take是一个action，会触发任务的提交
//      val tuples: Array[((String, String), Int)] = filtered.sortBy(_._2,false).take(3)
      val tuples: Array[((String, String), Int)] = filtered.sortBy(_._2,false).take(3)

      //打印
      println(tuples.toBuffer)
    }

    sc.stop()
  }
}
