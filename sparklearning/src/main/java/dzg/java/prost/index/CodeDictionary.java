package dzg.java.prost.index;

import java.util.ArrayList;
import java.util.List;

/**
 * @author BelieverDzg
 * @date 2020/12/9 14:38
 */
public class CodeDictionary {

    public static void main(String[] args) {

        //获取不同的谓语

        //获取不同得主语节点

        //获取不同的宾语节点

        //根据上述数据进行编码，将字符串映射为唯一的id <String,Integer>，然后将其进行调换位置，获取编码字典<Integer,String>

        //构建SG-Index
        List<SGIndex> index = new ArrayList<>();

        String s = "<http://db.uwaterloo.ca/~galuc/wsdbm/Offer1002>";
        /*
        将s和p合并计算hash值，作为key，然后将o的hash值作为value，快速检索
         */
        System.out.println(s.length());
    }

}
