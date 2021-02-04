package dzg.java.prost.trieIndex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 假设节点只包含小写字母
 */
public class TrieNode implements Serializable {


    private static final long serialVersionUID = 8778222676945871793L;
    Map<Character, TrieNode> links;
    boolean isEnd;

    //Trie树自身已经存储了字符串
//    String related;

    //主语标识的end位置 ， 设置其对应的 objs
//    Set<String> objs; // 一对多

    //在宾语标识为end的位置 ， 设置其对应的subjects
//    Set<String> subs;

    public TrieNode() {
        links = new HashMap<>();
        //怎么查找呢 ？？？
//        objs = new HashSet<>();
//        subs = new HashSet<>();
    }

    public Map<Character, TrieNode> getLinks() {
        return links;
    }

    public void setLinks(Map<Character, TrieNode> links) {
        this.links = links;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

//    @Override
////    public String toString() {
////        return "TrieNode{" +
////                "li" + links +
////                ", is=" + isEnd +
////                '}';
////    }

    /*    public String getRelated() {
        return related;
    }

    public void setRelated(String related) {
        this.related = related;
    }

    @Override
    public String toString() {
        return "TrieNode{" +
                "links=" + links +
                ", isEnd=" + isEnd +
                ", related='" + related + '\'' +
                '}';
    }*/

    /* private final int SIZE = 26;

    //一个节点的分支
    private TrieNode[] links;

    //标记是否表示字符串的结尾 或者只是表示节点的前缀
    private boolean isEnd;

    public TrieNode() {
        links = new TrieNode[SIZE];
    }

    //判断是否包含某个字符节点
    public boolean containsKey(char ch) {
        return links[ch - 'a'] != null;
    }

    //获取指定位置的节点
    public TrieNode get(char ch) {
        return links[ch - 'a'];
    }

    //添加一个新的链接节点
    public void put(char ch, TrieNode node) {
        links[ch - 'a'] = node;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean isEnd) {
        this.isEnd = isEnd;
    }

    @Override
    public String toString() {
        return "TrieNode{" +
                "  links=" + Arrays.toString(links) +
                ", isEnd=" + isEnd +
                '}';
    }*/
}
