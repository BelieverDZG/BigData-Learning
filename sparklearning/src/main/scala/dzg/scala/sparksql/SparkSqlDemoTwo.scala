package dzg.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/29 16:50
 *
 *      1.x中方法二：
 *
 *       1)创建SparkContext，然后创建SQLContext
 *       2)先创建RDD，对数据进行整理，然后关联row,将非结构化数据转换成结构化数据
 *       3)定义schema
 *       4）调用sqlContext的createDataFrame方法
 *       5）注册临时表
 *       6）执行sql（Transformation，lazy）
 *       7）执行Action
 */
object SparkSqlDemoTwo {

  def main(args: Array[String]): Unit = {

    //提交的这个程序可以连接到Spark集群中
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlDemoTwo").setMaster("local[*]")
    //创建SparkSQL的链接（程序执行的入口）
    val sc = new SparkContext(conf)
    //SparkContext不能创建特殊的RDD，如DataFrame

    val sqlContext = new SQLContext(sc)

    //创建特殊的RDD,即DataFrame，就是有schema信息的RDD

    //现有一个普通的RDD，然后关联上schema，进而转换成DataFrame
    val lines: RDD[String] = sc.textFile("hdfs://10.196.83.229:9000/person")
    //    val lines: RDD[String] = sc.textFile(args(0))

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Row(id, name, age, fv)
    })

    //结果类型，其实就是表头用于描述DataFrame
    val schema = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    //将rowRDD关联schema
    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, schema)


    //    bdf.registerTempTable("t_boy")
    //
    //    //编写sql（SQL方法其实是Transformation）
    //    val result: DataFrame = sqlContext.sql("select * from t_boy order by fv desc,age asc")
    //
    //    result.show()

    /**
     * 不使用SQL的方式，就不用注册临时表了
     * bdf.select("name")
     */
    val df1: DataFrame = bdf.select("name", "age", "fv")

    import sqlContext.implicits._
    val df2: Dataset[Row] = df1.orderBy($"fv" desc, $"age" asc)

    df1.show()

    df2.show()

    sc.stop()

  }

}
