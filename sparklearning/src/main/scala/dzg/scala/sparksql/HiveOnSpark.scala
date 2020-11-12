package dzg.scala.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 测试下Hive运行在Spark上
 *
 * @author BelieverDzg
 * @date 2020/11/6 15:36
 */
object HiveOnSpark {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      .enableHiveSupport()////启用spark对hive的支持(可以兼容hive的语法了)
      .getOrCreate()
    //想要使用hive的元数据库，必须指定hive元数据的位置，添加一个hive-site.xml到当前程序的classpath下即可
//    val sqlFrame: DataFrame = spark.sql("select * from users")
//    sqlFrame.show()

    val frame: DataFrame = spark.sql("create table t_test(id bigint,age bigint,name string)")
    System.setProperty("HADOOP_USER_NAME","root")
//    frame.explain()
    frame.show()
    spark.close()
  }
}
