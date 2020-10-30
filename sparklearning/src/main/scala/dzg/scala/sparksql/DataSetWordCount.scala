package dzg.scala.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author BelieverDzg
 * @date 2020/10/29 21:28
 */
object DataSetWordCount {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("SQLWordCount")
      .master("local[*]")
      .getOrCreate()

    //指定从那里读取数据，lazy读取
    //DataSet分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //dataSet只有一列，默认这一列叫做value
    val lines: Dataset[String] = session.read.textFile("hdfs://10.196.83.229:9000/dzg/wc.txt")
    lines.show()
    //切分压平
    import session.implicits._ //导入隐式转换
    val words: Dataset[String] = lines.flatMap(line => line.split(" "))

    //使用DataSet的API（DSL）
//    val count: DataFrame = words.groupBy($"value" as "word").count().sort($"count" desc)
    val frame: DataFrame = words.groupBy($"value" as( "word")).count()
    val num: Long = frame.count()
    //    count.show()

    //导入聚合函数
    import org.apache.spark.sql.functions._
    val value: Dataset[Row] = words.groupBy($"value" as "word")
      .agg(count("*") as "counts")
      .orderBy($"counts" desc)
    value.show()

    session.stop()
  }
}
