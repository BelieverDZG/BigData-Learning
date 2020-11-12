package dzg.scala.sparksql

import java.nio.file.Files

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

/**
 * 分析对比ROC和Parquet对数据的压缩处理的效率
 *
 * @author BelieverDzg
 * @date 2020/11/3 15:30
 */
object ORCAndParquetCompare {


  def main(args: Array[String]): Unit = {

    val start: Long = System.currentTimeMillis()

    val conf: SparkConf = new SparkConf().setAppName("ORCAndParquetCompare").setMaster("local[*]")

    val spark = new SparkContext(conf)

    val sqlContext = new SQLContext(spark)

    //读取数据
    val lines: RDD[String] = spark.textFile("D:/RDF数据/watdiv.100M/watdiv.100M.nt")

    //    println(lines.collect())
    val rdfRDD: RDD[Row] = lines.map(line => {
      val strs: Array[String] = line.split("\t")
      val subject: String = strs(0).trim
      val predicate: String = strs(1).trim
      val obj: String = strs(2).trim
      Row(subject, predicate, obj)
    })

//    val subPartitioned: RDD[(String, Iterable[(String, String, String)])] = rdfRDD.groupBy(rdf => rdf._1)
//
//    for (elem <- subPartitioned.collect()) {
//      println(elem)
//    }

    //添加表头信息，注册schema信息
    val schema = StructType(List(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("age", StringType, true)
    ))

    val df: DataFrame = sqlContext.createDataFrame(rdfRDD,schema)

//    df.show()
    var i = 0
    while(i < 10){

    }
//    df.write.format("parquet").save("d:/exp/watdiv.100m/parquet_3")
    df.write.format("orc").save("d:/exp/watdiv.100m/orc_3")
    val end: Long = System.currentTimeMillis() - start

    /**
     * parquet 用时：4910 毫秒
     * orc 用时：4340 毫秒
     */
    println(s"用时：$end 毫秒")
    spark.stop()

  }

  private def demo = {

    val spark: SparkSession = SparkSession.builder()
      .appName("ORCAndParquetCompare")
      .master("local[*]")
      //      .enableHiveSupport() //启动spark对hive的支持，使其兼容hive的语法 、导入依赖
      .getOrCreate()
    import spark.implicits._
    val squaresDF: DataFrame = spark.sparkContext.makeRDD(1 to 5)
      .map(i => (i, i * i))
      .toDF("value", "square")
    squaresDF.write.parquet("d:/data/test_table/key=1")

    val cubesDF: DataFrame = spark.sparkContext.makeRDD(6 to 10)
      .map(i => (i, i * i * i))
      .toDF("value", "square")
    cubesDF.write.parquet("d:/data/test_table/key=2")

    //读取分区后的表格
    val mergerDF: DataFrame = spark.read.option("mergeSchema", "true").parquet("d:/data/test_table")
    mergerDF.printSchema()
    mergerDF.show()

    spark.close()
  }

  def m(): Unit = {

  }
}


