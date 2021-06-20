package com.dzg.basicoperation;

import com.alibaba.fastjson.JSON;
import com.dzg.pojo.Player;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;


import java.io.IOException;
import java.util.*;


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
    void indexApiTest() throws IOException {

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("posts")
                .id("3").source(builder);
        indexRequest.source("",XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
    }

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
        GetRequest getRequest = new GetRequest("posts", "1");

        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

        Map<String, Object> source = response.getSourceAsMap();

        for (Map.Entry<String, Object> entry : source.entrySet()) {
            System.out.println(entry.getKey() + " , " + entry.getValue());
        }
    }

    /**
     * GetSourceRequest 专门用于获取文档的_source资源
     *
     * @throws IOException
     */
    @Test
    void getSourceTest() throws IOException {
        //专用于获取某一个文档的_source资源信息
        GetSourceRequest getSourceRequest = new GetSourceRequest("posts", "1");
        GetSourceResponse sourceResponse = client.getSource(getSourceRequest, RequestOptions.DEFAULT);
        sourceResponse.getSource().forEach((key, value) -> System.out.println(key + " , " + value));
    }

    /**
     * 更新文档信息
     * @throws IOException
     */
    @Test
    void updateDocTest() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test", "1");
        updateRequest.timeout(TimeValue.timeValueMillis(10000));

        //使用脚本来更新文档信息(有错误，待解决)
        // Caused by: ElasticsearchException[Elasticsearch exception [type=null_pointer_exception, reason=null]]
        Map<String, Object> parameters = Collections.singletonMap("count", 4);

        //该脚本可以作为内联脚本提供,或者存储脚本ScriptType.STORED
        Script inline = new Script(ScriptType.INLINE, "painless",
                "ctx._source.counter += params.count", parameters);
        updateRequest.script(inline);

//        updateRequest.upsert() 更新的文档可能不存在，则将更新内容新建为一个文档索引
        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());

        //根据数据的映射，更新对应的值
       /* HashMap<String, Object> update = new HashMap<>();
        update.put("name","Lilard2");
        update.put("birth","1996-11-03");
        updateRequest.doc(update);*/

    }


    @Test
    void termVectorsRequestTest() throws IOException {
        TermVectorsRequest request = new TermVectorsRequest("posts", "1");
        request.setFields("user345");
        TermVectorsResponse response = client.termvectors(request, RequestOptions.DEFAULT);
    }
    @Test
    void deleteDocTest() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("posts", "2");
        deleteRequest.timeout("1s");
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

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
    void mulGetTest() throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (int i = 1; i <= 1; i++) {
            multiGetRequest.add(new MultiGetRequest.Item("test",String.valueOf(i))
                    .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));
        }
        //mget: Retrieves multiple documents by id using the Multi Get API.
        MultiGetResponse mget = client.mget(multiGetRequest, RequestOptions.DEFAULT);
        MultiGetItemResponse[] responses = mget.getResponses();
        for (MultiGetItemResponse respons : responses) {
            System.out.println(respons.getResponse().getSourceAsString());
        }
    }

    @Test
    void updateByQueryTest() throws IOException {

        UpdateByQueryRequest request = new UpdateByQueryRequest("test");
        request.setMaxDocs(10);

        BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
        long created = bulkByScrollResponse.getCreated();
        System.out.println(created);
    }

    @Test
    void searchTest() throws IOException {

        SearchRequest searchRequest = new SearchRequest("dzg_index");
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();匹配所有查询
        //HighlightBuilder高亮查询
        //QueryBuilder
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