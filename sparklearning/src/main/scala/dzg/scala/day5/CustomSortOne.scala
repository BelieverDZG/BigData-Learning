package dzg.scala.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/26 20:38
 *
 *       用户自定义排序
 */
object CustomSortOne {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSortOne").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //排序规则：首先按照颜值降序，相等的话则按照年龄升序
    val arr = Array("zhangsan 30 99", "lisi 29 9999", "wangwu 28 99", "wangba 30 99", "zhangsan 30 999")

    //将Driver端的数据并行化为RDD
    val lines: RDD[String] = sc.parallelize(arr)

    //切分整理
    val tpRDD: RDD[User] = lines.map(line => {
      val strs: Array[String] = line.split(" ")
      val name: String = strs(0)
      val age: Int = strs(1).toInt
      val fv: Int = strs(2).toInt
      new User(name, age, fv)
    })
    //将RDD里面的封装类型的数据进行排序
    val sorted: RDD[User] = tpRDD.sortBy(u => u)

    val users: Array[User] = sorted.collect()

//    println(users.toBuffer)

    /**
     * name: lisi , age: 29, fv: 9999
     * name: zhangsan , age: 30, fv: 999
     * name: wangwu , age: 28, fv: 99
     * name: zhangsan , age: 30, fv: 99
     * name: wangba , age: 30, fv: 99
     */
    users.foreach(a => println(a))
    sc.stop()
  }
}

class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {

  override def toString: String = s"name: $name , age: $age, fv: $fv"

  override def compare(that: User): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      that.fv - this.fv
    }


  }
}
