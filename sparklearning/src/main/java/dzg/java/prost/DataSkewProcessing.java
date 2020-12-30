package dzg.java.prost;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.collection.mutable.ArrayBuffer;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *  map-side-join
 *
 * @author BelieverDzg
 * @date 2020/12/25 13:15
 */
public class DataSkewProcessing {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("DataSkewProcessing");
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        List<List<String>> info = new ArrayList<>();
        info.add(Arrays.asList("110","lsw"));
        info.add(Arrays.asList("222","yyy"));

        JavaRDD<List<String>> peopleRDD = sc.parallelize(info);

        List<List<String>> studentAll = new ArrayList<>();
        studentAll.add(Arrays.asList("110","s1","211"));
        studentAll.add(Arrays.asList("111","s2","222"));
        studentAll.add(Arrays.asList("112","s3","233"));
        studentAll.add(Arrays.asList("113","s2","244"));

        JavaRDD<List<String>> studentRDD = sc.parallelize(studentAll);

        Broadcast<JavaRDD<List<String>>> broadcastPeople = sc.broadcast(peopleRDD);


//        studentRDD.mapPartitions(listIterator -> {
//            JavaRDD<List<String>> bcValue = broadcastPeople.value();
//            ArrayBuffer<Object> oab = new ArrayBuffer<>();
//            listIterator.forEachRemaining((idCard,school,sno) -> {
//                if (bcValue)
//            });
//        });

        sc.stop();
    }
}
