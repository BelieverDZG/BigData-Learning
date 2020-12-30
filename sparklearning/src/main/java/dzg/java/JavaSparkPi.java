package dzg.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

/**
 * @author BelieverDzg
 * @date 2020/11/26 20:02
 */
public class JavaSparkPi {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        int slices = 2;
        int n = 10000 * slices;
        ArrayList<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(list, slices);

        Integer count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + x * y <= 1) ? 1 : 0;
        }).reduce((a, b) -> a + b);
        System.out.println("Pi is roughly " + 4.0 * count / n);
        spark.stop();
    }
}
