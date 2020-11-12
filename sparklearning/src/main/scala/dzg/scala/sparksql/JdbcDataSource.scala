package dzg.scala.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row, SparkSession}

/**
 * @author BelieverDzg
 * @date 2020/10/30 21:06
 */
object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()


    val reader: DataFrameReader = spark.read.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://localhost:3306/bigdata?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=true",
      "dirver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "logs",
      "user" -> "root",
      "password" -> "root"
    ))

    val logs: DataFrame = reader.load()

    logs.printSchema()

    logs.show()

    import spark.implicits._
    /*

    val filtered: Dataset[Row] = logs.filter(r => {
      r.getAs[Int](2) <= 25
    })
    filtered.show()
    */

    //使用lambda表达式
    val filtered: Dataset[Row] = logs.filter($"age" <= 25)
    filtered.show()

    val result: DataFrame = filtered.select($"id",$"name",$"age" * 10 as "new_age")
    result.show()

    //将上述过滤数据写入数据库新的表格
    /*val props = new Properties()
    props.put("user","root")
    props.put("password","root")
    result.write
      .mode("ignore") // 表存在则不做任何操作，否则创建表并写入数据
      .jdbc("jdbc:mysql://localhost:3306/bigdata?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=true",
       "logcopy", props)*/

    //只能保存字符串类型的一列数据到文本文件
    //result.write.text("")
    //保存为json文件
//    result.write.json("d:/saved_json")
//    result.write.csv("d:/saved_csv")
//    result.write.parquet("d:/saved_parquet")
    result.write.orc("d:/saved_orc")
    spark.stop()
  }
}
