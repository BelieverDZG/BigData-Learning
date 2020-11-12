package dzg.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/29 16:50
 *
 *      1.x中方法一：
 *
 *       1)创建SparkContext，然后创建SQLContext
 *       2)先创建RDD，对数据进行整理，然后关联case class，解非结构化数据转化为结构化数据
 *       3)显示的调用toDF方法，将RDD转换为DataFrame
 *       4）注册临时表
 *       5）执行sql（Transformation，lazy）
 *       6）执行Action
 */
object SparkSqlBySQLContextDemo {

  def main(args: Array[String]): Unit = {
    //提交的这个程序可以连接到Spark集群中
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlDemoOne").setMaster("local[*]")
    //创建SparkSQL的链接（程序执行的入口）
    val sc = new SparkContext(conf)
    //SparkContext不能创建特殊的RDD，如DataFrame

    val sqlContext = new SQLContext(sc)

    //创建特殊的RDD,即DataFrame，就是有schema信息的RDD

    //现有一个普通的RDD，然后关联上schema，进而转换成DataFrame
    val lines: RDD[String] = sc.textFile("hdfs://10.196.83.229:9000/person")
    //    val lines: RDD[String] = sc.textFile(args(0))

    val boyRDD: RDD[Boy] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Boy(id, name, age, fv)
    })

    //结果类型，其实就是表头用于描述DataFrame

    //该RDD装的是Boy类型的数据，有了schema信息，但还是一个RDD
    //将RDD转换成DataFrame
    //导入隐式转换
    import sqlContext.implicits._
    val bdf: DataFrame = boyRDD.toDF()

    //变成DF后，就可以使用两种API进行编程了
    bdf.registerTempTable("t_boy")

    //编写sql（SQL方法其实是Transformation）
    val result: DataFrame = sqlContext.sql("select * from t_boy order by fv desc,age asc")

    result.show()

    sc.stop()
  }


}

case class Boy(id: Long, name: String, age: Int, fv: Double)

