package dzg.scala.sparksql

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 用户自定义聚合函数测试
 *
 * @author BelieverDzg
 * @date 2020/10/30 15:09
 */
object DefineAggregateFunction {
  //几何平均数 ，乘积后开根号
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DefineAggregateFunction")
      .master("local[*]")
      .getOrCreate()
    val nums: Dataset[lang.Long] = spark.range(1, 11)

    val gm = new GeoMean
    //注册函数
    //    spark.udf.register("gm", gm);
    //    nums.createTempView("v_range")
    //    nums.show()
    //    val result: DataFrame = spark.sql("select gm(id) result from v_range")
    import spark.implicits._
    val result: Dataset[Row] = nums.agg(gm($"id")).as("geomean")
    result.show()

    spark.stop()
  }
}

class GeoMean extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = StructType(List(
    StructField("value", DoubleType)
  ))

  //产生中间结果数据类型
  override def bufferSchema: StructType = StructType(List(
    //相乘之后返回的积
    StructField("product", DoubleType),
    StructField("counts", LongType)
  ))

  //最终返回结果
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  //执行初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //参与相乘的初始值
    buffer(0) = 1.0
    //参与运算数字的个数
    buffer(1) = 0L
  }

  //每一条数据参与运算就更新一下中间结果（update相当于在每一个分区中的运算）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //每有一个数字参与运算就进行相乘（包含中间结果）
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
    //更新参与运算的数据个数
    buffer(1) = buffer.getLong(1) + 1L
  }

  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每一个分区计算的结果进行相乘
    buffer1(0) = buffer1.getDouble(0) * buffer2.getDouble(0)
    //每个分区参与运算的中间结果进行相加
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(0), 1.toDouble / buffer.getLong(1))
  }
}
