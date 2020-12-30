package dzg.java.prost;

import java.util.HashMap;
import java.util.Map;

/**
 * @author BelieverDzg
 * @date 2020/12/8 19:37
 */
public class MergeMap {

    public static void main(String[] args) {
        Map<Integer,Integer> mapOne = new HashMap<>();
        mapOne.put(1,10);
        mapOne.put(2,20);
        mapOne.put(3,30);
        Map<Integer,Integer> mapTwo = new HashMap<>();
        mapTwo.put(3,30000);
        mapTwo.put(4,40);
        mapTwo.put(5,50);
        Map<Integer,Integer> mergedMap = new HashMap<>();

        mergedMap.putAll(mapOne);

        mergedMap.putAll(mapTwo);

        for (Map.Entry<Integer, Integer> entry : mergedMap.entrySet()) {
            System.out.println(entry.toString());
        }
    }
}
