package dzg.java.prost;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Sequence;
import org.spark_project.jetty.util.ConcurrentHashSet;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author BelieverDzg
 * @date 2020/12/15 18:14
 */
public class FindSPOCharacter {

    //查找出知识图谱中的所有字符
    static List<Character> distinctChars = new ArrayList<>(1024 * 1024);

    private static Set<String> uris = new ConcurrentHashSet<>();

    //subject-objs
    static Map<String, List<String>> maps = new ConcurrentHashMap<>();

    public static void main(String[] args) {

//        SparkConf conf = new SparkConf()
//                .setAppName("FindSPOCharacter")
//                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("FindSPOCharacter")
                .getOrCreate();

        //求出字符串集合
        Dataset<String> dataset = spark.read().textFile("D:\\RDF数据\\watdiv.10M\\watdiv.10M.nt");

        Dataset<Row> df = dataset.toDF();

        df.printSchema();
        long begin = System.currentTimeMillis();
        df.foreach((ForeachFunction<Row>) row -> {
            if (row.size() < 0) return;
            String[] splits = row.getString(0).split("\t");
            for (String str : splits) {
                if (!str.startsWith("<")) continue;
                if (str.endsWith(".")) {
                    uris.add(str.substring(0, str.length() - 1).trim());
                } else {
                    uris.add(str);
                }
            }
        });
        long totalTime = System.currentTimeMillis() - begin;
        System.out.println(uris);
        System.out.println("use " + totalTime + " ms");
        System.out.println(uris.size());

//      System.out.println(distinctChars);
        spark.stop();
    }


    public static void findOne2OneRelation() {
        Integer[] tupleArray = {69970,150000,149998,7530,50095,344,20128,4491142,17899,7554,842
        ,150000,5000};
    }
}
