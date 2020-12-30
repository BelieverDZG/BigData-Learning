package dzg.java.prost;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;

/**
 * @author BelieverDzg
 * @date 2020/11/25 16:26
 */
public class ParquetAndOrcComparison {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("compare parquet and orc file format")
                .getOrCreate();

//        SparkConf conf = new SparkConf().setMaster("").setAppName("");
//        SparkContext sc = new SparkContext(conf);

        //DataFrame
        Dataset<String> tripleFile = spark.read().textFile("D:\\RDF数据\\watdiv.100M\\watdiv.100M.nt");

        long begin = System.currentTimeMillis();
//        orcAndParquetWriteTest(tripleFile);
//        orcWriteTest(spark.newSession(),tripleFile);
        parquetWriteTest(spark.newSession(), tripleFile);
        System.out.println("用时：" + (System.currentTimeMillis() - begin) / 1000 + " 秒");
        spark.stop();

    }

    /**
     * 第一次：
     * [30819, 24307, 25293, 23072, 13532, 26114, 25352, 26703, 39841, 28494]
     * parquet写十次，平均时间为 26352 毫秒
     *
     * [28187, 28320, 24735, 27519, 25053, 29995, 25260, 27912, 27136, 25144]
     * orc写十次，平均时间为 26926 毫秒
     *
     *
     * 第二次：
     * ==========================10M的数据=============================
     * [26495, 26996, 27252, 26928, 25002, 27253, 25277, 25610, 25310, 26236]
     * parquet写十次，平均时间为 26235 毫秒
     * avgSize = 182M
     *
     * [24160, 26711, 23911, 26351, 25202, 24510, 29088, 26519, 29070, 25886]
     * orc写十次，平均时间为 26140 毫秒
     * avgSize = 169M
     *
     * 523
     *
     * ============================100M的数据===============================
     *
     * [251630, 251559, 251711, 252119, 258998, 253104, 263016, 262474, 258389, 262482]
     * parquet写十次，平均时间为 256548 毫秒
     *
     * [274729, 280757, 291695, 283365, 276605, 285567, 281848, 266601, 281699, 279064]
     * orc写十次，平均时间为 280193 毫秒
     * 用时：5367 秒
     *
     * parquet 1.87GB
     *
     * ORC 1.74GB
     *
     *
     * @param tripleFile
     */

    /**
     * [250980, 257381, 259965, 242937, 248864, 250077, 247102, 255921, 268393, 273153]
     * orc写十次，平均时间为 255477 毫秒
     * <p>
     * 1.74GB
     * <p>
     * [248146, 253711, 256187, 250078, 260215, 251554, 251769, 248827, 252862, 257838]
     * orc写十次，平均时间为 253118 毫秒
     *
     * @param spark
     * @param tripleFile
     */
    private static void orcWriteTest(SparkSession spark, Dataset<String> tripleFile) {
        //分别存储十次，计算平均时间
        long orcSumTime = 0;
        long[] writeOrcPer = new long[10];

        //orc写测试
        for (int i = 0; i < writeOrcPer.length; i++) {
            long begin = System.currentTimeMillis();
            tripleFile.write().orc("d:/experiment100/orc_100M_" + i);
            writeOrcPer[i] = System.currentTimeMillis() - begin;
            orcSumTime += writeOrcPer[i];
        }

        System.out.println(Arrays.toString(writeOrcPer));
        System.out.println("orc写十次，平均时间为 " + (orcSumTime / 10) + " 毫秒");

        spark.stop();
    }

    /**
     * [258277, 250236, 267193, 261390, 263933, 267459, 260465, 268678, 263424, 267593]
     * parquet写十次，平均时间为 262864 毫秒
     *
     * @param spark
     * @param tripleFile
     */
    private static void parquetWriteTest(SparkSession spark, Dataset<String> tripleFile) {
        long parquetSumTime = 0;
        long[] writeParquetPer = new long[10];
        //parquet写测试
        for (int i = 0; i < writeParquetPer.length; i++) {
            long begin = System.currentTimeMillis();
            tripleFile.write().parquet("d:/experiment100/parquet_100M_" + i);
            writeParquetPer[i] = System.currentTimeMillis() - begin;
            parquetSumTime += writeParquetPer[i];
        }
        System.out.println(Arrays.toString(writeParquetPer));
        System.out.println("parquet写十次，平均时间为 " + (parquetSumTime / 10) + " 毫秒");
        spark.stop();
    }


    public static void orcAndParquetReadTest(SparkSession spark) {

        File file = new File("d:/test");
        if (file.isDirectory()) {
            File[] files = file.listFiles();
        }

        long begin = System.currentTimeMillis();
        Dataset<Row> orc = spark.read().orc();
        long total = System.currentTimeMillis() - begin;
        long avg = total / 10;
        System.out.println("读取ORC文件平均时间为：" + avg + "秒");

        spark.read().parquet();

        spark.stop();
    }


}