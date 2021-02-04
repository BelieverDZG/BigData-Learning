package dzg.java.prost;

import java.util.*;

/**
 * @author BelieverDzg
 * @date 2021/2/4 21:26
 */
public class MathTest {

    public static void main(String[] args) {
        double a = 1.1;
        double b = 1;
        //a - b : 1.0
        //b - a : -0.0
        System.out.println((int) Math.ceil(b - a));

        List<Double> values = new ArrayList<>();

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            values.add((double) random.nextInt(100));
        }

        System.out.println(values.toString());

        PriorityQueue<Double> nodesQueue = new PriorityQueue<>(values.size(), new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                return (int) Math.ceil(o1 - o2);
            }
        });

        nodesQueue.addAll(values);

        System.out.println(nodesQueue);
    }
}
/*

class MyComparator implements Comparator<Double>{

    @Override
    public int compare(Double o1, Double o2) {
        return Math.ceil(o1.doubleValue() - o2.doubleValue());
    }
}*/
