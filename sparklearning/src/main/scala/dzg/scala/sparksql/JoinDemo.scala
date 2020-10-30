package dzg.scala.sparksql

import org.apache.avro.generic.GenericData.StringType
import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author BelieverDzg
 * @date 2020/10/30 9:53
 *
 */
object JoinDemo {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("JoinDemo")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
    val lines: Dataset[String] = session.createDataset(List(
      "1,Liard,USA", "2,JermeryLin,China", "3,Kobe,USA", "4,YAO,China"
    ))

    //对数据进行整理
    val value: Dataset[(Int, String, String)] = lines.map(line => {
      val arr: Array[String] = line.split(",")
      val id: Int = arr(0).toInt
      val name: String = arr(1)
      val nation: String = arr(2)
      (id, name, nation)
    })

    val df1: DataFrame = value.toDF("id", "name", "hometown")

    val rule: Dataset[String] = session.createDataset(List("China,中国", "USA,美国"))

    val countryRule: Dataset[(String, String)] = rule.map(line => {
      val arr: Array[String] = line.split(",")
      val ename: String = arr(0)
      val cname: String = arr(1)
      (ename, cname)
    })

    val df2: DataFrame = countryRule.toDF("ename", "cname")

    df1.show()
    df2.show()

    //第一种方法，创建视图
    df1.createTempView("t_people")
    df2.createTempView("t_nations")
    val joinRes: DataFrame = session.sql("select id,name,cname from t_people JOIN t_nations on hometown = ename")
    joinRes.show()

    //    根据RDD创建的
    //    val schema = StructType(List(
    //      StructField("ID", IntType, true),
    //      StructField("name", StringType, true),
    //      StructField("hometown", StringType, true)
    //    ))
    //
    //    table.createTempView("t_people",schema)
    //
    //    table.show()

    //第二种方法，使用join的方式
    val joinDf: DataFrame = df1.join(df2, $"hometown" === $"ename", "right_outer")

    joinDf.show()
    session.stop()
  }
}
