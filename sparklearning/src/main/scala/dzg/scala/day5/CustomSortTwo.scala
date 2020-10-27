package dzg.scala.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/26 20:38
 *
 *      用户自定义排序
 */
object CustomSortTwo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSortTwo").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //排序规则：首先按照颜值降序，相等的话则按照年龄升序
    val arr = Array("zhangsan 30 99" , "lisi 29 9999","wangwu 28 99", "wangba 30 99")

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
    //将RDD里面的封装类型的数据进行排序
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => new Boy(tp._2,tp._3))

    val tuples: Array[(String, Int, Int)] = sorted.collect()

    println(tuples.toBuffer)

    sc.stop()
  }
}

class Boy(val age: Int,val fv : Int) extends Ordered[Boy] with Serializable {
  override def toString: String = s"age: $age, fv: $fv"

  override def compare(that: Boy): Int = {
    if(this.fv == that.fv){
      this.age - that.age
    }else{
      that.fv - this.fv
    }
  }
}
