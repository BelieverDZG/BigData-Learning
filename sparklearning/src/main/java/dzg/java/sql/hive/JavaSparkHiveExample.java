package dzg.java.sql.hive;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.STRING;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author BelieverDzg
 * @date 2020/11/23 10:15
 */
public class JavaSparkHiveExample {

    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("CREATE TABLE IF NOT EXISTS src(key int, value String) using hive");
//        spark.sql("load data local inpath 'src/main/resources/kv1.txt' into table src");
        //使用HiveQL对表格进行操作
        spark.sql("select * from src").show();
        //聚合函数操作
        spark.sql("select count(*) from src").show();

        Dataset<Row> sqlDF = spark.sql("select key , value from src where key < 10 order by key");

        Dataset<String> stringDS = sqlDF.map((MapFunction<Row, String>) row -> "key: " + row.get(0) + ",value: " + row.get(1), Encoders.STRING());

        stringDS.show();

        //使用DataFrame创建临时视图
        List<Record> records = new ArrayList<>();
        Record record;
        for (int i = 1; i < 100; i++) {
            record = new Record(i, "val_" + i);
            records.add(record);
        }
        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");
        spark.sql("select * from records r join src s on r.key = s.key limit 10").show();
        spark.stop();
    }

    //example on spark hive
    public static class Record implements Serializable {
        private int key;
        private String value;

        public Record(int key, String value) {
            this.key = key;
            this.value = value;
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }
    }
}
