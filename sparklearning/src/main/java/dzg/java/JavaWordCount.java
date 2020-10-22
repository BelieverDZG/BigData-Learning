package dzg.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
 * @date 2020/10/22 15:28
 */
public class JavaWordCount {

    public static void main(String[] args) {
        //0、运行环境配置
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
        //1、创建SparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //2、指定从哪里读取文件数据 args[0] 程序入口
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //3、切分文件数据并压平
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        //4、将单词组合在一起
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        //5、聚合

        JavaPairRDD<String, Integer> reducedWords = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //6、按照单词出现的次数进行排序，先调换元组顺序进行排序，然后再调换回来
        JavaPairRDD<Integer, String> swaped = reducedWords.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        });
        //降序排列
        JavaPairRDD<Integer, String> sort = swaped.sortByKey(false);
        JavaPairRDD<String, Integer> sortedWord = sort.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
            }
        });
        //7、打印数据或者存储到HDFS中
        sortedWord.foreach(a -> System.out.println(a));
        //8、释放资源
        jsc.stop();

    }
}
