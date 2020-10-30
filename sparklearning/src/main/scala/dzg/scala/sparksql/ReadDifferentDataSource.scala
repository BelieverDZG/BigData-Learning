package dzg.scala.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author BelieverDzg
 * @date 2020/10/30 21:43
 *
 *      读取不同的数据源
 */
object ReadDifferentDataSource {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("ReadDifferentDataSource")
      .master("local[*]")
      .getOrCreate()

    //文件读取数据时会检验文件数据的校验和
    //若源文件被修改，进行校验和检验的时候发现不同，会报错
    //Caused by: org.apache.hadoop.fs.ChecksumException: Checksum error: file:
    val jsonFrame: DataFrame = spark.read.json("d:/saved_json")
    jsonFrame.show()

    val csvFrame: DataFrame = spark.read.csv("d:/saved_csv")
    csvFrame.show()
    csvFrame.printSchema()

    val df: DataFrame = csvFrame.toDF("id","name","age")
    df.show()

    /**
     * parquet是列式存储的文件格式
     *
     * parquet文件既保存了数据，又保存了schema信息
     *
     * 保存了列，列的类型，偏移量信息
     */
    //    val parquetFrame: DataFrame = spark.read.parquet("d:/saved_parquet")
    val parquetFrame: DataFrame = spark.read.format("parquet").load("d:/saved_parquet")
    parquetFrame.printSchema()
    parquetFrame.show()

    spark.stop()
  }
}
