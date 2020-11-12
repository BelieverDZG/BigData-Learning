package dzg.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BelieverDzg
 * @date 2020/10/29 16:50
 *
 *      2.x中方法：
 *
 *       1)创建SparkSession
 *       2)通过SparkSession对象创建RDD，并map数据
 *       3)定义schema
 *       4）通过session对象，创建DataFrame
 *       5）执行SQL操作，获取DataSet
 *       6）show或者是保存上一步的执行结果
 *       7）关闭session
 */
object SparkSqlBySparkSessionDemo{

  def main(args: Array[String]): Unit = {

    //spark2.x SQL编程API（SparkSession）
    val session: SparkSession = SparkSession.builder()
      .appName("SparkSqlDemoThree")
      .master("local[*]")
      .getOrCreate()

    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://10.196.83.229:9000/person")

    //整理数据,map -> 创建schema，即表头
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Row(id, name, age, fv)
    })

    val schema = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    //创建DataFrame
    val frame: DataFrame = session.createDataFrame(rowRDD, schema)

    import session.implicits._
    val res: Dataset[Row] = frame.where($"fv" > 90).orderBy($"fv" desc, $"age" asc)
    res.show()

    session.stop()
  }
}
