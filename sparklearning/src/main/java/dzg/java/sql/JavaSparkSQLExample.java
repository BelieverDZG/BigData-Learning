package dzg.java.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.col;

/**
 * @author BelieverDzg
 * @date 2020/11/23 10:51
 */
public class JavaSparkSQLExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Java Spark SQL example")
                .getOrCreate();
//        runBasicDataFrameExample(spark);
//        runDataSetCreationExample(spark);
//        runInferSchemaExample(spark);
//        runProgrammaticSchemaExample(spark);
        Dataset<Row> queryRes = spark.read().parquet("hdfs://hadoop101:9000/res");
        System.out.println(queryRes.count());
        queryRes.show();
        spark.stop();
    }

    private static void runProgrammaticSchemaExample(SparkSession spark){
        //创建RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("src/main/resources/people.txt", 1)
                .toJavaRDD();
        //schema 暂时存储在字符串中
        String schemaString  = "name age";
        //基于schema字符串生成schema
        ArrayList<StructField> fields = new ArrayList<>();
        for (String fieldName: schemaString.split(" ")){
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);
        //将记录转化为Row
        JavaRDD<Row> rowRDD = peopleRDD.map(record -> {
            String[] attrs = record.split(" ");
            return RowFactory.create(attrs[0], attrs[1].trim());
        });
        //将模式应用到RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        //创建临时视图
        peopleDataFrame.registerTempTable("people");

        Dataset<Row> results = spark.sql("select name from people");

        //spark sql查询的结果集时DataFrame,其支持所有RDD的操作

        Dataset<String> namesDS = results.map((MapFunction<Row, String>) row ->
                "Name: " + row.getString(0), Encoders.STRING());

        namesDS.show();
    }

    private static void runInferSchemaExample(SparkSession spark) {
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        //将schema应用到RDD上，创建DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.registerTempTable("people");

        //使用spark sql操作表people
        Dataset<Row> teenagerDF = spark.sql("select name from people where age between 13 and 19");
        //每一行中的列元素可以通过索引下标来获取
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagerDF.map((MapFunction<Row, String>) row ->
                "Name: " + row.getString(0), stringEncoder);
        teenagerNamesByIndexDF.show();

        //或者通过属性来获取
        Dataset<String> teenagerNamesByFieldDF = teenagerDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
    }

    private static void runDataSetCreationExample(SparkSession spark) {
        // $example on:create_ds$
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();

        //类编码器中提供了大多数常见类型的编码器
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3, 4), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value ->
                value + 1, integerEncoder);

        transformedDS.show();

        //可以通过提供类将DataFrame转换为DataSet。基于名称的映射
        String path = "src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
    }

    public static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");
        df.show();
        df.printSchema();

        df.select(col("name"), col("age").plus(1).as("plusAge")).show();

        df.filter(col("age").gt(21)).show();

        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("select * from people");
        sqlDF.show();

        //全局视图被保存在预先设置的数据库 global_temp中 , 全局视图时跨session的
        df.createGlobalTempView("people");
        spark.sql("select * from global_temp.people").show();

        spark.newSession().sql("select * from global_temp.people").show();
    }

    public static
    class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
