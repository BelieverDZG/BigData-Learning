package com.dzg.util;

import com.dzg.pojo.Product;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 解析html页面的工具
 * @author BelieverDzg
 * @date 2021/6/11 13:47
 */

@Component
public class HtmlParseUtil {

    public static List<Product> getProduct(String keyword) throws IOException {
        //1、获取请求 https://search.jd.com/Search?keyword=java
        String url = "https://search.jd.com/Search?keyword="+keyword;

        //2、解析网页: Jsoup返回Document就是浏览器Document对象
//        Document document = Jsoup.parse(new URL(url), 50000);
        Document document = Jsoup.connect(url).userAgent("Chrome").timeout(30000).get();

        //3、所有在js中使用的方法，这里都可以使用: 获取所有li元素
        Element element = document.getElementById("J_goodsList");

        Elements liEle = element.getElementsByTag("li");

        List<Product> products = new ArrayList<>();
        //4、获取元素中的内容，这里的el 就是每一个li标签
        for (Element el : liEle) {
            String img = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();
            products.add(new Product(img,title,price));
        }

        return products;
    }


}
