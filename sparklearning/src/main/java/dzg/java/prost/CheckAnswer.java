package dzg.java.prost;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Sequence;
import org.codehaus.janino.Java;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author BelieverDzg
 * @date 2020/11/27 20:33
 */
public class CheckAnswer {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().
                master("local[*]")
        .appName("Check SPARQL query answer")
        .getOrCreate();

        List<Integer> list = Arrays.asList(1, 2, 3);
        SparkContext sparkContext = spark.sparkContext();
        JavaRDD<Integer> parallelize = new JavaSparkContext(sparkContext).parallelize(list);
        System.out.println(parallelize.partitions().size());
        System.out.println(parallelize);
//
//        Dataset<Row> queryAnswer = spark.read().parquet("hdfs://hadoop101:9000/user/hive/warehouse/watdiv.db/properties");
//        Dataset<Row> orcTT = spark.read().orc("hdfs://hadoop101:9000/user/hive/warehouse/watdivorc.db/tripletable");
////        queryAnswer.write().csv("d:/");
//        queryAnswer.show();
//
////        queryAnswer.write().csv("d:/t");
//        System.out.println(queryAnswer.count());
//
//        //orc table
//        orcTT.show();
//        System.out.println(orcTT.count());
        spark.stop();
    }
}
