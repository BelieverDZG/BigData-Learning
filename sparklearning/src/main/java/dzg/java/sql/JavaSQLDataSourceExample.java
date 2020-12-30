package dzg.java.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author BelieverDzg
 * @date 2020/11/23 12:38
 */
public class JavaSQLDataSourceExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .master("local[*]")
                .getOrCreate();
//        runBasicDataSourceExample(spark);
        runParquetSchemaMergingExample(spark);
        spark.stop();
    }

    private static void runParquetSchemaMergingExample(SparkSession spark) {
        List<Square> squares = new ArrayList<>();
        for(int val = 1; val <= 5; val++){
            Square square = new Square();
            square.setValue(val);
            square.setSquare(val*val);
            squares.add(square);
        }
        //创建一个简单的DataFrame，存入一个分区目录
        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().parquet("data/test_table/key=1");
        squaresDF.show();
        squaresDF.printSchema();

        List<Cube> cubes = new ArrayList<>();
        for (int i = 6; i < 11; i++) {
            Cube cube = new Cube();
            cube.setValue(i);
            cube.setCube(i*i*i);
            cubes.add(cube);
        }
        // Create another DataFrame in a new partition directory,
        // adding a new column and dropping an existing column
        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().parquet("data/test_table/key=2");
        cubesDF.show();
        cubesDF.printSchema();

        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
        mergedDF.show();
        mergedDF.printSchema();
    }
    private static void runBasicDataSourceExample(SparkSession spark) {
        // $example on:generic_load_save_functions$
        Dataset<Row> usersDF = spark.read().load("src/main/resources/users.parquet");
        usersDF.show();
        usersDF.select("name", "favorite_color")
                .write()
                .save("nameAndFavColors.parquet");

        // $example off:generic_load_save_functions$
        // $example on:manual_load_options$
        Dataset<Row> peopleDF =
                spark.read().format("json").load("src/main/resources/people.json");
        peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");

        // $example off:manual_load_options$
        // $example on:manual_load_options_csv$
        Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/main/resources/people.csv");
        // $example off:manual_load_options_csv$
        // $example on:manual_save_options_orc$
        usersDF.write().format("orc")
                .option("orc.bloom.filter.columns", "favorite_color")
                .option("orc.dictionary.key.threshold", "1.0")
                // $example off:manual_save_options_orc$
                .save("users_with_options.orc");

        // $example on:direct_sql$
        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`");
        // $example off:direct_sql$
        // $example on:write_sorting_and_bucketing$
        peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");
        // $example off:write_sorting_and_bucketing$
        // $example on:write_partitioning$
        usersDF
                .write()
                .partitionBy("favorite_color")
                .format("parquet")
                .save("namesPartByColor.parquet");
        // $example off:write_partitioning$
        // $example on:write_partition_and_bucket$
        peopleDF
                .write()
                .partitionBy("favorite_color")
                .bucketBy(42, "name")
                .saveAsTable("people_partitioned_bucketed");
        // $example off:write_partition_and_bucket$

        spark.sql("DROP TABLE IF EXISTS people_bucketed");
        spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed");
    }

    // $example on:schema_merging$
    public static class Square implements Serializable {
        private int value;
        private int square;

        // Getters and setters...
        // $example off:schema_merging$
        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getSquare() {
            return square;
        }

        public void setSquare(int square) {
            this.square = square;
        }
        // $example on:schema_merging$
    }
    // $example off:schema_merging$

    // $example on:schema_merging$
    public static class Cube implements Serializable {
        private int value;
        private int cube;

        // Getters and setters...
        // $example off:schema_merging$
        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getCube() {
            return cube;
        }

        public void setCube(int cube) {
            this.cube = cube;
        }
        // $example on:schema_merging$
    }
}
