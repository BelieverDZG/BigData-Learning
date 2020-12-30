package dzg.scala.sparksql

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * @author BelieverDzg
 * @date 2020/11/22 20:23
 */
object SparkSqlSevenJoin {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().
      master("local[*]").appName("SparkSqlJoin")
      .getOrCreate()

    val deptDF: DataFrame = spark.read.json("src/main/resources/dept.json")
    deptDF.createTempView("dept")
    val empDF: DataFrame = spark.read.json("src/main/resources/emp.json")
    empDF.createOrReplaceTempView("emp")

    val joinExpression: Column = empDF.col("deptno") === deptDF.col("deptno")

    /**
     * 1、Inner Join
     *
     * empDF.join(deptDF,joinExpression).select("ename","dname").show()
     * spark.sql("select e.ename,d.dname from emp e , dept d where e.deptno = d.deptno").show()
     * spark.sql("select e.ename,d.dname from emp e inner join dept d on e.deptno = d.deptno").show()
     */


    /**
     * 2、full outer join
     * empDF.join(deptDF,joinExpression,"outer").show()
     * spark.sql("select * from dept d full outer join emp e on d.deptno = e.deptno")
     */

    /**
     * 3、left outer join
     *     empDF.join(deptDF,joinExpression,"left_outer").show()
     *         spark.sql("select * from emp left outer join dept on emp.deptno = dept.deptno").show()
     *
     */

    /**
     * 4、right outer join
     *     empDF.join(deptDF,joinExpression,"right_outer").show()
     *     spark.sql("select * from emp right outer join dept on emp.deptno = dept.deptno").show()
     */

    /**
     * 5、left semi join
     *     empDF.join(deptDF,joinExpression,"left_semi").show()
     *     spark.sql("select * from emp e left semi join dept d on e.deptno = d.deptno").show()
     *     spark.sql("select * from emp where deptno in(select deptno from dept)").show()
     */

    /**
     * 6、left anti join
     *     empDF.join(deptDF,joinExpression,"left_anti").show()
     *     spark.sql("select * from emp where deptno not in (select deptno from dept)").show()
     *     spark.sql("select * from emp left anti join dept on emp.deptno = dept.deptno").show()
     */

    /**
     * 7、cross join
     *     empDF.join(deptDF, joinExpression, "cross").show()
     *    val lines: Long = empDF.join(deptDF, joinExpression, "cross").count()
     *    println(lines) //14
     */
    empDF.join(deptDF,joinExpression,"cross").explain()
    println(spark.sql("select * from dept cross join emp").count())
    spark.stop()
  }
}
