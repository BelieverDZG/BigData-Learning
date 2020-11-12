package dzg.scala.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 *
 * 测试spark sql join 的操作
 *
 * join原理解析：http://sharkdtu.com/posts/spark-sql-join.html
 *
 * @author BelieverDzg
 * @date 2020/11/8 14:38
 */
object SparkSqlJoinDemo {

  val conf: SparkConf = new SparkConf().setAppName("SparkSqlJoinDemo").setMaster("local[*]")

  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  var df1: DataFrame = null
  var df2: DataFrame = null
  var df3: DataFrame = null

  def main(args: Array[String]): Unit = {

    createDataFrame
    /*
    //inner join
    val innerJoinFrame: DataFrame = df1.join(df2, "name")
    //val innerJoinFrame: DataFrame = df1.join(df2, "name")
    innerJoinFrame.orderBy("id") show()
    //内连接多个参数
    df1.join(df3,Seq("id","name"),"inner").select("id","name","age","height").orderBy("id").show()
    */
    //full outer join (在df1中没有Rose的相关信息，那么相应的列位置信息为null)
    val outerJoinFrame: DataFrame = df1.join(df2, Seq("name"), "outer")
    outerJoinFrame.select("id", "name", "age", "height").orderBy("id").show()

    //左外连接
    df1.join(df2, Seq("name"), "left").select("*").orderBy("id").show()
    //右外连接
    df1.join(df2, Seq("name"), "right").select("*").orderBy("id").show()
    spark.stop()
  }

  private def createDataFrame = {
    //用于RDD与DF之间的隐式转换
    //创建DataFrame
    val rddOne: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Array((1, "Alice", 18), (2, "Andy", 19),
      (3, "Bob", 17), (4, "Justin", 21), (5, "Cindy", 20)))

    import spark.implicits._
    /**
     * +---+------+---+
     * | id|  name|age|
     * +---+------+---+
     * |  1| Alice| 18|
     * |  2|  Andy| 19|
     * |  3|   Bob| 17|
     * |  4|Justin| 21|
     * |  5| Cindy| 20|
     * +---+------+---+
     */
    df1 = rddOne.toDF("id", "name", "age")
    //    df1.printSchema()
    //    df1.show()

    //创建DF2
    /**
     * +-----+------+
     * | name|height|
     * +-----+------+
     * |Alice|   160|
     * | Andy|   169|
     * |  Bob|   170|
     * |Cindy|   180|
     * | Rose|   160|
     * +-----+------+
     */
    val rddTwo: RDD[(String, Int)] = spark.sparkContext.parallelize(Array(("Alice", 160), ("Andy", 169), ("Bob", 170), ("Cindy", 180), ("Rose", 160)))
    df2 = rddTwo.toDF("name", "height")
    //    df2.show()

    //创建DF3
    /**
     * +---+------+------+
     * | id|  name|height|
     * +---+------+------+
     * |  1| Alice|   160|
     * |  2|  Andy|   159|
     * |  3|   Tom|   175|
     * |  4|Justin|   171|
     * |  5| Cindy|   165|
     * +---+------+------+
     */
    val rddThree: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Array((1, "Alice", 160), (2, "Andy", 159), (3, "Tom", 175), (4, "Justin", 171), (5, "Cindy", 165)))
    df3 = rddThree.toDF("id", "name", "height")
    //    df3.show()
  }
}
