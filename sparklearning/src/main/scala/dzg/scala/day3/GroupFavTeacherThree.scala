package dzg.scala.day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author BelieverDzg
 * @date 2020/10/27 15:14
 *
 *      自定义分区器
 */
object GroupFavTeacherThree {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacherThree").setMaster("local[4]")

    val context = new SparkContext(conf)

    val lines: RDD[String] = context.textFile(args(0))

    val keyAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val idx: Int = line.lastIndexOf("/")
      val name: String = line.substring(idx + 1)
      val url: String = line.substring(0, idx)
      val subject: String = new URL(url).getHost.split("[.]")(0)
      ((subject, name), 1)
    })

    val reduced: RDD[((String, String), Int)] = keyAndOne.reduceByKey(_+_)

    //计算有多少个学科
    val subjects: RDD[String] = reduced.map(_._1._1)
    val subs: Array[String] = subjects.distinct().collect()

    //自定义一个分区器，并且按照指定的分区器进行分区
    val sbPartitioner = new SubjectPartitioner(subs)

    //partitionBy按照指定的分区规则进行分区
    //调用partitionBy时RDD的Key是(String, String)
//    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPartitioner)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPartitioner)

    //如何一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(3).iterator
    })
    val tuples: Array[((String, String), Int)] = sorted.collect()

    println(tuples.toBuffer)

    context.stop()
  }
}

class SubjectPartitioner(sbs: Array[String]) extends Partitioner{

  //相当于主构造器（new的时候回执行一次）
  //用于存放规则的一个map
  private val rules = new mutable.HashMap[String,Int]()
  var i = 0
  for(sb <- sbs){
    rules.put(sb,i)
    i += 1
  }

  //返回分区的数量---下一个RDD有多少分区
  override def numPartitions: Int = sbs.length

  //根据传入的key计算分区标号
  //key是一个元组（String， String）
  override def getPartition(key: Any): Int = {
    //学科名称
    val value: String = key.asInstanceOf[(String,String)]._1
    //根据规则计算分区编号
    rules(value)
  }
}
