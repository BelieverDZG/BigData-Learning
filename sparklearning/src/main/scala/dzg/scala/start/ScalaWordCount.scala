package dzg.scala.start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/19 15:28
 *      生成了6个RDD
 *      根据案例图示
 *          生成两种Task，两个Stage，再shuffle之前的是一种ShuffleMapTask，shuffle之后是另一种ResultTask
 *          4个Task
 */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //调用textFile生成了HadoopRDD[K,V]和 MapPartitionsRDD[String]
    val lines: RDD[String] = sc.textFile(args(0))

    //生成一个MapPartitionsRDD[String]
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //生成一个MapPartitionsRDD[(String,Int)]
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //生成一个ShuffledRDD[(String,Int)]
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    val sorted: RDD[(String,Int)] = reduced.sortBy(_._2, false)

    //MapPartitionsRDD[NullWritable,Text]
//    sorted.saveAsTextFile(args(1))

    sorted.foreach(a => println(a))
    sc.stop()
  }

  def iterfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next()
    while (iter.hasNext) {
      val cur = iter.next()
      res ::= (pre, cur)
      pre = cur
    }
    res.iterator
  }


  //  iterfunc: [T](iter:Iterator[T])Iterator[(T,T)]
}
