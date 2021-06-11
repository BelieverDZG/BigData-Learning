package dzg.java.prost.queryanalysis;

import com.opencsv.CSVReader;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author BelieverDzg
 * @date 2021/3/3 10:08
 */
public class QueryResultAnalysis {


    public static void main(String[] args) {

        String wat100MWithoutBr = "D:\\RDF数据\\实验结果数据\\watdiv100mRes\\" +
                "watdiv100MRes\\watdiv100M_VP_WPT_minGroup-1_randomOrder_without_br.csv";

        watdiv100MAverage(wat100MWithoutBr);

        String wat100MWithBr = "D:\\RDF数据\\实验结果数据\\watdiv100mRes\\" +
                "watdiv100MRes\\watdiv100M_VP_WPT_minGroup-1_randomOrder_br.csv";

        String wat1BWithBr = "D:\\RDF数据\\实验结果数据\\watdiv1BRes\\" +
                "watdiv1BRes\\watdiv1B_VP_WPT_minGroup-1_randomOrder_10_without_br.csv";

        String yago2WithoutBroadcastJoin = "D:\\RDF数据\\实验结果数据\\yagoQuery\\" +
                "yagoQuRes\\yago2_VP_WPT_minGroup-1_randomOrder_without_br.csv";

        String yago2WithBroadcastJoin = "D:\\RDF数据\\实验结果数据\\yagoQuery\\" +
                "yagoQuRes\\yago2_VP_WPT_minGroup-1_randomOrder_br.csv";

        String yago2WithBroadcastJoin2 = "D:\\RDF数据\\实验结果数据\\yagoQuery\\" +
                "yagoQuRes\\yago2_VP_WPT_minGroup-1_randomOrder.csv";

//        String yago2MinGroup2WithBroadcastJoin = "D:\\RDF数据\\实验结果数据\\yagoQuery\\yagoQuRes\\yago2_VP_WPT_minGroup-2_randomOrder.csv";
//
//        System.out.println("yago2_VP_WPT_minGroup-1_randomOrder_without_br.csv analysis : ");
//        yago2Analysis(yago2WithoutBroadcastJoin);
//
//        System.out.println();
//
//        System.out.println("yago2_VP_WPT_minGroup-1_randomOrder_br.csv");
//        yago2Analysis(yago2WithBroadcastJoin);
//
//        System.out.println("yago2_VP_WPT_minGroup-2_randomOrder.csv");
//        yago2Analysis(yago2MinGroup2WithBroadcastJoin);

//        System.out.println("yago2_VP_WPT_minGroup-1_randomOrder-br.csv execute 8 times");
//        yago2Analysis(yago2WithBroadcastJoin2);

//        System.out.println("without broadcast join");
//        watdiv100MAverage(wat100MWithoutBr);

//        System.out.println("求解WatDiv1B的查询效率");
//        watdiv100MAverage(wat1BWithBr);
//
//        System.out.println("with broadcast join");
//        watdiv100MAverage(wat100MWithBr);



        String watdiv1B_VP_only_g1_br = "D:\\RDF数据\\watdiv1B_VP_minGroup-1_randomOrder.csv";
//        watdiv100MAverage(watdiv1B_VP_only_g1_br);

        String watdiv1B_BP_WPT_g1_without_br = "D:\\RDF数据\\实验结果数据\\watdiv1BRes\\watdiv1BRes\\watdiv1B_VP_WPT_minGroup-1_randomOrder_without_br.csv";
//        watdiv100MAverage(watdiv1B_BP_WPT_g1_without_br);
    }

    private static void demo() {
        String csvFile = "D:\\RDF数据\\实验结果数据\\watdiv1000_TT_VP_WPT_minGroup-1_randomOrder.csv";

        long lTime = 0, sTime = 0, cTime = 0, fTime = 0;
        Map<String, Long> time = new TreeMap<>();
        CSVReader reader = null;

        try {
            reader = new CSVReader(new FileReader(csvFile));
            reader.readNext();
            String[] line;
            while ((line = reader.readNext()) != null) {

                String query = line[0];
                long consumedTime = Long.parseLong(line[1]);
                query = query.substring(query.lastIndexOf('/') + 1);
//                System.out.println("[query= " + query + ", numberOfResults= " + numOfResults);
                time.put(query, time.getOrDefault(query, 0L) + consumedTime);


                switch (query.substring(0, 1)) {
                    case "L":
                        lTime += consumedTime;
                        break;
                    case "S":
                        sTime += consumedTime;
                        break;
                    case "F":
                        fTime += consumedTime;
                        break;
                    default:
                        cTime += consumedTime;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Long> query : time.entrySet()) {
            System.out.println(" " + query.getKey() + " , " + query.getValue());
        }


        System.out.println("L time " + lTime + " ms");
        System.out.println("S time " + sTime + " ms");
        System.out.println("F time " + fTime + " ms");
        System.out.println("C time " + cTime + " ms");
    }


    public static void watdiv100MAverage(String filePath) {

        int totalQuery = 0;
        long lTime = 0, sTime = 0, cTime = 0, fTime = 0;
        long lTotal = 0, sTotal = 0, cTotal = 0, fTotal = 0;
        CSVReader reader = null;

        Map<String, Long> time = new TreeMap<>();

        Map<String, Long> perQueryTotal = new TreeMap<>();

        Map<String, Integer> countPerQuery = new TreeMap<>();

        try {
            reader = new CSVReader(new FileReader(filePath));
            reader.readNext();
            String[] line;
            while ((line = reader.readNext()) != null) {
                totalQuery++;
                String query = line[0].substring(line[0].lastIndexOf('/') + 1);
                long consumedTime = Long.parseLong(line[1]);
                //每一个的查询总时间（不同的查询）
                time.put(query, time.getOrDefault(query, 0L) + consumedTime);

                //每一个查询的执行次数
                countPerQuery.put(query, countPerQuery.getOrDefault(query, 0) + 1);

                //每一种（L1、L2、、、、）查询的执行时间
                String q = query.substring(0, 2);
                perQueryTotal.put(q, perQueryTotal.getOrDefault(q, 0L) + consumedTime);

                switch (query.substring(0, 1)) {
                    case "L":
                        lTime += consumedTime;
                        lTotal++;
                        break;
                    case "S":
                        sTime += consumedTime;
                        sTotal++;
                        break;
                    case "F":
                        fTime += consumedTime;
                        fTotal++;
                        break;
                    default:
                        cTime += consumedTime;
                        cTotal++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println("sparql查询语句的总数：" + totalQuery);
        System.out.println("total query " + lTotal + "  L time " + lTime / lTotal + " ms");
        System.out.println("total query " + sTotal + "  S time " + sTime / sTotal + " ms");
        System.out.println("total query " + fTotal + "  F time " + fTime / fTotal + " ms");
        System.out.println("total query " + cTotal + "  C time " + cTime / cTotal + " ms");

        System.out.println(time.size());
        for (Map.Entry<String, Long> query : time.entrySet()) {
            System.out.println(" " + query.getKey() + " , " + query.getValue());
        }

//        for (Map.Entry<String, Integer> perQuery : countPerQuery.entrySet()) {
//            System.out.println(" " + perQuery.getKey() + " , " + perQuery.getValue());
//        }

        for (Map.Entry<String, Long> query : perQueryTotal.entrySet()) {
            System.out.println(" " + query.getKey() + " , " + query.getValue());
        }
    }


    public static void yago2Analysis(String filePath){
        long y1Time = 0, y2Time = 0, y3Time = 0, y4Time = 0;
        Map<String, Long> time = new TreeMap<>();
        CSVReader reader = null;

        try {
            reader = new CSVReader(new FileReader(filePath));
            reader.readNext();
            String[] line;
            while ((line = reader.readNext()) != null) {

                String query = line[0];
                long consumedTime = Long.parseLong(line[1]);
                query = query.substring(query.lastIndexOf('/') + 1);
//                System.out.println("[query= " + query + ", numberOfResults= " + numOfResults);
                time.put(query, time.getOrDefault(query, 0L) + consumedTime);

/*
                switch (query.substring(0, 1)) {
                    case "L":
                        lTime += consumedTime;
                        break;
                    case "S":
                        sTime += consumedTime;
                        break;
                    case "F":
                        fTime += consumedTime;
                        break;
                    default:
                        cTime += consumedTime;
                }*/
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Long> query : time.entrySet()) {
            System.out.println(" " + query.getKey() + " , " + query.getValue());
        }

//
//        System.out.println("L time " + lTime + " ms");
//        System.out.println("S time " + sTime + " ms");
//        System.out.println("F time " + fTime + " ms");
//        System.out.println("C time " + cTime + " ms");
    }
}
