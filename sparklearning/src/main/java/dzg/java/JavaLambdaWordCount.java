package dzg.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author BelieverDzg
 * @date 2020/10/22 15:47
 */
public class JavaLambdaWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaLambadaWordCount").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile(args[0]);

        JavaRDD<String> words
                = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

        words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                .mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) Tuple2::swap)
                .sortByKey(false)
                .mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) Tuple2::swap)
                .foreach(a -> System.out.println(a));

        jsc.stop();
    }
}
