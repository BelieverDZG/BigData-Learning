package dzg.scala.day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @Date 2020/10/26 20:07
 *
 *  求不同学科最受欢迎的老师
 */
object GroupFavTeacherOne {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacherOne").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

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

    //分组排序，按学科进行分组，统计不同老师对应的数据，然后进行排序
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    /**
     * (javaee,CompactBuffer(((javaee,xiaoxu),6), ((javaee,laoyang),9)))
     * (php,CompactBuffer(((php,laoliu),1), ((php,laoli),3)))
     * (bigdata,CompactBuffer(((bigdata,laozhang),2), ((bigdata,laozhao),15), ((bigdata,laoduan),6)))
     */
    grouped.foreach(a => println(a))

    //经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    //将每一个组拿出来进行操作
    //为什么可以调用scala的sortBy方法呢？
    // 因为一个学科的数据已经在一台机器上的一个scala集合里面了
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(2))

    val tuples: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(tuples.toBuffer)

    sc.stop()
  }
}
