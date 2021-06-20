package com.dzg.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 描述京东商城中一个商品的相关属性
 * @author BelieverDzg
 * @date 2021/6/14 13:18
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {

    private String img;
    private String title;
    private String price;
}
