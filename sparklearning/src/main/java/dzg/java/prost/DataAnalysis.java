package dzg.java.prost;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 分析json文件，vp表中数据倾斜问题，设置一个合理的阈值 ，使用broadcast join
 *
 * @author BelieverDzg
 * @date 2020/12/26 10:12
 */
public class DataAnalysis {

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("D:\\RDF数据\\实验结果数据\\watdiv1000Res\\watdiv1000.json");
        JsonNode rootNode;
        List<Integer> tuples = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        int sum = 0;
        int midValue = 0;
        double avg = 0;
        int trueNum = 0;
        int falseNum = 0;
        int inverseTrueNum = 0;
        int inverseFalseNum = 0;
        Set<String> complex = new HashSet<>();

        long distinctSubjects = 0 , distinctObjects = 0;

        try {
            rootNode = mapper.readValue(file, JsonNode.class);
            JsonNode databaseName = rootNode.path("databaseName");
            System.out.println(databaseName);
            String properties = rootNode.path("properties").asText();
            for (JsonNode jsonNode : rootNode.path("properties")) {
                boolean isComplex = jsonNode.path("isComplex").asBoolean();
                boolean isInverseComplex = jsonNode.path("isInverseComplex").asBoolean();

                JsonNode internalName = jsonNode.path("internalName");
                int tuplesNumber = jsonNode.path("tuplesNumber").asInt();

                //主语对宾语
                if (isComplex) {
                    trueNum++;
                } else {
                    complex.add(internalName.toString());
                    falseNum++;
                }

                //宾语对主语
                if (isInverseComplex){
                    inverseTrueNum++;
                }else {
                    inverseFalseNum++;
                }

                max = Math.max(max, tuplesNumber);
                min = Math.min(min, tuplesNumber);
                sum += tuplesNumber;
                tuples.add(tuplesNumber);

                distinctSubjects += jsonNode.path("distinctSubjects").asLong();
                distinctObjects += jsonNode.path("distinctObjects").asLong();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        for (int val : tuples){
//            System.out.println(val);
//        }


//        System.out.println("tuples size : " + tuples.size());
        tuples.sort(Integer::compareTo);
        int size = tuples.size();
        avg = sum / size;
        if ((size & 1) != 0) {
            midValue = tuples.get(size / 2 + 1);
        } else {
            midValue = (tuples.get(size / 2) + tuples.get(size / 2 + 1)) / 2;
        }

        System.out.println(complex);
        System.out.println("谓语个数"+tuples.size());
        System.out.println(tuples);
        System.out.println("最大元组数：" + max);
        System.out.println("最小元组数：" + min);
        System.out.println("平均元组数：" + avg);
        System.out.println("中位数---元组数：" + midValue);
        System.out.println("VP表为S->O为一对一的表的个数：" + falseNum + " 百分比：" + ((double) falseNum / size));
        System.out.println("VP表S->O不是一对一的表的个数：" + trueNum + " 百分比：" + (trueNum / (double) size));

        System.out.println("O -> S true num : " + inverseTrueNum);
        System.out.println("O -> S false num : " + inverseFalseNum);


        System.out.println("==============================");

        System.out.println("distinctSubjects : " + distinctSubjects);
        System.out.println("distinctObjects : " + distinctObjects);
    }
}
