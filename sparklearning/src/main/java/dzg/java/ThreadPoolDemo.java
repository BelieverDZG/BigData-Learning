package dzg.java;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author BelieverDzg
 * @date 2020/10/28 9:24
 */
public class ThreadPoolDemo {

    public static void main(String[] args) {
        //单线程线程池
//        ExecutorService pool = Executors.newSingleThreadExecutor();
        //核心线程数为5的线程池
//        ExecutorService pool = Executors.newFixedThreadPool(5);

        //可缓冲的线程池（可以有多个线程）
        ExecutorService pool = Executors.newCachedThreadPool();

        for (int i = 0; i < 10; i++) {
            pool.execute(() -> {
                System.out.println(Thread.currentThread().getName() + " starts");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + " is over");
            });
        }

        pool.shutdown();
    }
}
