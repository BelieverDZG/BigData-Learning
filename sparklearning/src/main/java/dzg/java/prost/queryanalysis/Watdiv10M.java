package dzg.java.prost.queryanalysis;


import com.opencsv.CSVReader;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.util.IOUtils;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author BelieverDzg
 * @date 2021/3/17 16:36
 */
public class Watdiv10M {

    public static void main(String[] args) throws IOException {
        //2021/3/17 WatDiv10M数据分析
//        System.out.println("watdiv10M-g1-with-br");
        String watdiv10Mg1WithBr = "D:\\RDF数据\\实验结果数据\\watdiv10M\\watdiv10M-g1-br.xlsx";

//        watdi10M(watdiv10Mg1WithBr);

        String watdiv10mg1WithoutBr = "D:\\RDF数据\\实验结果数据\\watdiv10M\\watdiv10M-g1-without-br.xlsx";
//        watdi10M(watdiv10mg1WithoutBr);

        String watdiv10Mg2WithoutBr = "D:\\RDF数据\\实验结果数据\\watdiv10M\\watdiv10M-G2-br.xlsx";
        watdi10M(watdiv10Mg2WithoutBr);

        String watdiv10Mg3WithoutBr = "D:\\RDF数据\\实验结果数据\\watdiv10M\\watdiv10M-g3-br.xlsx";
//        watdi10M(watdiv10Mg3WithoutBr);
    }

    private static void watdi10M(String path) throws IOException {
        Workbook workbook = null;

        int totalQuery = 0;

        long lTime = 0, sTime = 0, cTime = 0, fTime = 0;

        long lTotal = 0, sTotal = 0, cTotal = 0, fTotal = 0;

        Map<String, Long> time = new TreeMap<>();

        Map<String, Long> perQueryTotal = new TreeMap<>();

        Map<String, Integer> countPerQuery = new TreeMap<>();

        try {
            workbook = WorkbookFactory.create(new File(path));
            int numberOfSheets = workbook.getNumberOfSheets();
            Sheet sheet = workbook.getSheetAt(numberOfSheets - 1);
            Row row = null;
            String query = null;
            long consumedTime = 0;
            for (int i = sheet.getFirstRowNum() + 1; i < sheet.getPhysicalNumberOfRows(); i++) {
                row = sheet.getRow(i);
                query = row.getCell(0).getStringCellValue();
                query = query.substring(query.lastIndexOf('/') + 1);
                totalQuery++;

                consumedTime = (long) row.getCell(1).getNumericCellValue();
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

        } catch (InvalidFormatException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(workbook);
        }

        System.out.println("sparql查询语句的总数：" + totalQuery);
        System.out.println("total query " + lTotal + " total L time " + lTime + " avg L time" + lTime / lTotal + " ms");
        System.out.println("total query " + sTotal + " total S time " + sTime + " avg S time " + sTime / sTotal + " ms");
        System.out.println("total query " + fTotal + " total F time " + fTime + " avg F time" + fTime / fTotal + " ms");
        System.out.println("total query " + cTotal + " total C time " + cTime + " avg C time " + cTime / cTotal + " ms");

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


    public static void watdiv10MAverage(String filePath) {

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
}
