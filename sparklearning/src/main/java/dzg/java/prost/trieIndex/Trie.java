package dzg.java.prost.trieIndex;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark_project.jetty.util.ConcurrentHashSet;

import java.io.*;
import java.util.*;

/**
 * 实现前缀树
 *
 * @author BelieverDzg
 * @date 2020/12/14 20:21
 */
public class Trie implements Serializable{

    private static final long serialVersionUID = -2241884919869943307L;
    private static TrieNode root;

    private static Set<String> uris = new ConcurrentHashSet<>();

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[6]")
                .appName("TestNT2TrieSize")
                .getOrCreate();

        Trie trieRoot = new Trie();

        //求出字符串集合
        Dataset<String> dataset = spark.read().textFile("D:\\RDF数据\\watdiv.10M\\1000.nt");

        Dataset<Row> df = dataset.toDF();

        df.printSchema();
        long begin = System.currentTimeMillis();
        //测试将10M三元组字符串，全部放入一个前缀树中，占用空间大小以及花费的时间
        df.foreach((ForeachFunction<Row>) row -> {
            if (row.size() < 0) return;
            String[] splits = row.getString(0).split("\t");
            for (String str : splits) {
                if (str.endsWith(".")) {
                    trieRoot.insert(str.substring(0, str.length() - 1).trim());
                } else {
                    trieRoot.insert(str);
                }
            }
        });

        long totalTime = System.currentTimeMillis() - begin;
        //输出前缀树到外部文件
        System.out.println("use " + totalTime + " ms");
        //System.out.println(distinctChars);
        spark.stop();

        BufferedWriter bfw = null;
        try {
            String diskPath = "e:\\trieSize2.json";
            //以json的格式输出内容
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            bfw = new BufferedWriter(new FileWriter(diskPath));
            bfw.write(gson.toJson(trieRoot.root));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bfw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    public Trie() {
        this.root = new TrieNode();
    }

    public void insert(final String subject, final String object) {
        TrieNode node = root;
        char[] chs = subject.toCharArray();
        for (char ch : chs) {
            if (!node.links.containsKey(ch)) {
                node.links.put(ch, new TrieNode());
            }
            node = node.links.get(ch);
        }
        node.setEnd(true);
//        node.setRelated(object);
    }


    /**
     * 向Trie树中插入键
     * <p>
     * 从根部开始搜索它对应的第一个键值字符的链接
     * <p>链接存在。沿着链接移动到树的下一个子层，继续搜索下一个键字符</p>
     * <p>链接不存在。创建一个新的节点，并将其与父节点的链接相连，该链接与当前键字符相匹配</p>
     * <p>
     * O(m)
     *
     * @param word
     */
    public void insert(String word) {
        TrieNode node = root;
        char[] chs = word.toCharArray();
        for (char ch : chs) {
            if (!node.links.containsKey(ch)) {
                node.links.put(ch, new TrieNode());
            }
            node = node.links.get(ch);
        }
        node.setEnd(true);
        /*TrieNode node = root;
        for (int i = 0; i < word.length(); i++) {
            char curChar = word.charAt(i);
            if (node == null) throw new RuntimeException("node is null ,so can not execute put or get action");
            if (!node.containsKey(curChar)) {
                node.put(curChar, new TrieNode());
            }
            node = node.get(curChar);
        }
        node.setEnd(true);*/
    }

    /**
     * 在Trie中查找键：每一个键在字典树中表示为从根到内部节点或叶的路径
     * 用第一个键字符从根部开始，检查当前节点中与键字符对应的链接，有两种情况：
     * <p>
     * 存在链接：移动到该链接后面路径中的下一个节点，并继续搜索下一个键字符
     * 不存在链接：若已经没有键字符，且当前节点标记为isEnd，则返回true。否则有两种可能均返回false
     * <p>
     * 还有键字符剩余，但无法跟随Trie树的键路径，找不到键
     * 没有键字符剩余，且当前键字符没有被标记为isEnd
     * </p>
     * </p>
     * O(m)
     *
     * @param word
     * @return
     */
    public TrieNode searchPrefix(String word) {
        TrieNode node = root;
        for (char ch : word.toCharArray()) {
            if (node.links.containsKey(ch)) {
                node = node.links.get(ch);
            } else {
                return null;
            }
        }
        return node;
        /*TrieNode node = root;
        for (int i = 0; i < word.length(); i++) {
            char curChar = word.charAt(i);
            if (node.containsKey(curChar)) {
                node = node.get(curChar);
            } else {
            }
        }
        return node;*/
    }

    public boolean search(String word) {
        TrieNode node = searchPrefix(word);
        return node != null && node.isEnd();
    }

    public String getRelated(String subject) {
        TrieNode node = searchPrefix(subject);
        if (node != null && node.isEnd) {
//            return node.related;
        }
        return null;
    }

    /**
     * 查找Trie树中的键前缀
     * O(m)
     *
     * @param word
     * @return
     */
    public boolean startsWith(String word) {
        TrieNode node = searchPrefix(word);
        return node != null;
    }


}

