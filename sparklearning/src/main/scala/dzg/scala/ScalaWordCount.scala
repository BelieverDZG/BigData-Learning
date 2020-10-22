package dzg.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/19 15:28
 */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val words = lines.flatMap(_.split(" "))

    val wordAndOne = words.map((_,1))

    val reduced = wordAndOne.reduceByKey(_+_)

    val sorted = reduced.sortBy(_._2,false)

    sorted.saveAsTextFile(args(1))

    sc.stop()
  }

  def iterfunc [T](iter: Iterator[T]) : Iterator[(T,T)] ={
    var res = List[(T, T)]()
    var pre = iter.next()
    while (iter.hasNext){
      val cur = iter.next()
      res ::= (pre,cur)
      pre = cur
    }
    res.iterator
  }


//  iterfunc: [T](iter:Iterator[T])Iterator[(T,T)]
}
