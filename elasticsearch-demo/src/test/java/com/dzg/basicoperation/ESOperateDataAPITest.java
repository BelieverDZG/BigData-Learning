package com.dzg.basicoperation;

import com.alibaba.fastjson.JSON;
import com.dzg.pojo.Player;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试ES操作数据的基本API
 * @author BelieverDzg
 * @date 2021/5/30 15:35
 */
@SpringBootTest
public class ESOperateDataAPITest {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    public void createIndexTest() throws IOException {
        //创建索引请求
        CreateIndexRequest indexRequest = new CreateIndexRequest("dzg_index2");
        //客户端执行请求
        CreateIndexResponse createIndexResponse =
                //同步操作
                client.indices().create(indexRequest, RequestOptions.DEFAULT);

//        client.indexAsync()异步执行
        System.out.println(createIndexResponse);
    }

    /**
     * 判断某个索引是否存在
     */
    @Test
    public void existIndexTest() throws IOException {
        GetIndexRequest request = new GetIndexRequest("dzg_index");
        boolean exist = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exist);//true
    }

    @Test
    public void deleteIndexTest() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("dzg_index2");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    /**
     * 测试添加文档
     */
    @Test
    void createDocTest() throws IOException {

        Player player = new Player("Anthony",37,201);
        //请求索引
        IndexRequest request = new IndexRequest("dzg_index");
        //规则 put /index/_doc/id
        request.id(String.valueOf(1));
//        request.timeout(TimeValue.timeValueMillis(1));

        //将请转化为JSON字符串
        request.source(JSON.toJSONString(player),XContentType.JSON);
        //客户端发送请求，获取查询结果

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);


        System.out.println(request);
        System.out.println(response);
        //index {[dzg_index][_doc][1], source[{"{\"name\":\"Anthony\",\"age\":37,\"height\":201.0}":"JSON"}]}
        //IndexResponse[index=dzg_index,type=_doc,id=1,version=1,result=created,seqNo=0,primaryTerm=1,shards={"total":2,"successful":1,"failed":0}]
    }


    /**
     * 获取文档，判断是否存在 get /dzg_index/_doc/1
     */
    @Test
    void isExistDocTest() throws IOException {
        GetRequest getRequest = new GetRequest("dzg_index", "5");
        //不获取返回的_source的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists); //false
    }

    /**
     * 获取文档信息
     * @throws IOException
     */
    @Test
    void getDocTest() throws IOException {
        GetRequest getRequest = new GetRequest("dzg_index", "1");

        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, DocumentField> fields = response.getFields();

        for (Map.Entry<String, DocumentField> fieldEntry : fields.entrySet()) {
            System.out.println(fieldEntry.getKey() +" ," +fieldEntry.getValue() + " -----");
        }

        System.out.println("size: "+fields.size());
        System.out.println(response.getSourceAsString());
        System.out.println(response + "====");
//        client.close();
    }

    /**
     * 更新文档信息
     * @throws IOException
     */
    @Test
    void updateDocTest() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("index1", "1");
        updateRequest.timeout(TimeValue.timeValueMillis(1000));

        //根据数据的映射，更新对应的值
        HashMap<String, Object> update = new HashMap<>();
        update.put("name","Lilard2");
        update.put("birth","1996-11-03");
        updateRequest.doc(update);

        //更新整个对象
//        Player p = new Player("Kamellon Anthony3",37,203);
//        updateRequest.doc(JSON.toJSON(p),XContentType.JSON);

        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());

    }


    @Test
    void deleteDocTest() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test", "2");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
        System.out.println(deleteResponse.toString());
    }

    @Test
    void bulkRequestDocTest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueSeconds(10));
        List<Player> players = new ArrayList<>();
        players.add(new Player("blazer1",21,211));
        players.add(new Player("blazer2",22,212));
        players.add(new Player("blazer3",23,213));
        players.add(new Player("blazer4",24,214));
        players.add(new Player("blazer5",25,215));
        players.add(new Player("blazer6",25,215));
        players.add(new Player("blazer7",25,215));
        players.add(new Player("blazer8",25,215));
        int n = players.size();
        for (int i = 0; i < n; i++) {
            bulkRequest.add(new IndexRequest("dzg_index")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(players.get(i)),XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());//false
    }

    @Test
    void searchTest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("dzg_index");

        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //精确查找
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "Anthony");

        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(TimeValue.timeValueMillis(1000));
        //指定搜索源
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println(JSON.toJSONString(hits));
    }


}
