package dzg.java;


import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * 从Java使用Spark状态api的示例。
 *
 * @author BelieverDzg
 * @date 2020/11/26 18:56
 */
public class JavaStatusTrackerDemo {

    public static final String APP_NAME = "JavaStatusTrackerDemo";

    public static final class IdentityWithDelay<T> implements Function<T, T> {

        @Override
        public T call(T v1) throws Exception {
            Thread.sleep(2000);
            return v1;
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("local[1]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        //为一个简单的工作实现进度报告器的示例。
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(
                new IdentityWithDelay<>()
        );
        JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
        while (!jobFuture.isDone()) {
            //sleep 1 second
            Thread.sleep(1000);
            List<Integer> jobIds = jobFuture.jobIds();
            if (jobIds.isEmpty()) continue;
            Integer currentJobId = jobIds.get(jobIds.size() - 1);
            SparkJobInfo jobInfo = jsc.statusTracker().getJobInfo(currentJobId);
            SparkStageInfo stageInfo = jsc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
            System.out.println(stageInfo.numTasks() + "tasks total: " + stageInfo.numActiveTasks() +
                    " active, " + stageInfo.numCompletedTasks() + "complete");
        }

        System.out.println("Job results are: " + jobFuture.get());
        spark.stop();
    }
}
