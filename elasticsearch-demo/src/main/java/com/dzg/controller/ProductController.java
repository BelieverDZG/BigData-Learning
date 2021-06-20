package com.dzg.controller;

import com.dzg.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author BelieverDzg
 * @date 2021/6/14 13:28
 */
@RestController
public class ProductController {

    @Autowired
    private ProductService productService;

    @GetMapping("/parse/{keyword}")
    public boolean parse(@PathVariable("keyword")String keyword) throws IOException {
        return productService.parseContent(keyword);
    }

    @GetMapping("/search/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String,Object>> search(
            @PathVariable("keyword")String keyword,
            @PathVariable("pageNo")int pageNo,
            @PathVariable("pageSize")int pageSize) throws IOException {
        return productService.searchPage(keyword,pageNo,pageSize);
    }

    @GetMapping("/searchHighlighter/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String,Object>> searchWithHighlighter(
            @PathVariable("keyword")String keyword,
            @PathVariable("pageNo")int pageNo,
            @PathVariable("pageSize")int pageSize) throws IOException {
        return productService.searchPageWithHighlighter(keyword,pageNo,pageSize);
    }
}
