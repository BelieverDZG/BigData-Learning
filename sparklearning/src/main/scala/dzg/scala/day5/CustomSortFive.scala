package dzg.scala.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/26 20:38
 *
 *       借助元组的比较规则，实现排序的效果
 *
 */
object CustomSortFive {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSortFive").setMaster("local[4]")

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
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(u => (-u._3, u._2))

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
