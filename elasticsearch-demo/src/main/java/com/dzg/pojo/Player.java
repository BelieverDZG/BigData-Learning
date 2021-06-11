package com.dzg.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 用于测试ES添加文档数据
 * @author BelieverDzg
 * @date 2021/5/30 16:28
 */
@Data
@Component
@AllArgsConstructor
@NoArgsConstructor
public class Player {
    private String name;
    private int age;
    private double height;
}
