package com.dzg.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring的两步骤：
 * 1、找对象
 * 2、放入Spring容器中待用
 *
 * @author BelieverDzg
 * @date 2021/5/30 15:29
 */
@Configuration
public class ElasticSearchClientConfig {

    @Bean
    public RestHighLevelClient restHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                new HttpHost("127.0.0.1",9200,"http")
        ));
        return client;
    }
}
