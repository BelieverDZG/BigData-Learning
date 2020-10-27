package dzg.scala.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/26 20:38
 *
 *       使用隐式参数实现排序
 *
 */
object CustomSortSix {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("CustomSortSix").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //排序规则：首先按照颜值降序，相等的话则按照年龄升序
    val arr = Array("zhangsan 30 99", "lisi 29 9999", "wangwu 28 99", "wangba 30 99", "zhangsan 30 999")

    //将Driver端的数据并行化为RDD
    val lines: RDD[String] = sc.parallelize(arr)

    //切分整理
    val tpRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val strs: Array[String] = line.split(" ")
      val name: String = strs(0)
      val age: Int = strs(1).toInt
      val fv: Int = strs(2).toInt
      (name, age, fv)
    })
    //充分利用元组的比较规则：先比较第一个，如果相等，再比较第二个 ，负的表示降序
    //Ordering[(Int, Int)]最终比较的规则格式
    //on[(String, Int, Int)] 未比较之前的数据格式
    //(t => (-t._3, t._2))怎样将规则转换成想要比较的格式
    implicit val rules = Ordering[(Int, Int)].on[(String, Int, Int)](t => (-t._3, t._2))

    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(u => u)

    val users: Array[(String, Int, Int)] = sorted.collect()

    //    println(users.toBuffer)

    /**
     * (lisi,29,9999)
     * (zhangsan,30,999)
     * (wangwu,28,99)
     * (zhangsan,30,99)
     * (wangba,30,99)
     */
    users.foreach(a => println(a))

    sc.stop()
    
  }
}
