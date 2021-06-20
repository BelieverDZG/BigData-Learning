package com.dzg.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * @author BelieverDzg
 * @date 2021/6/11 13:35
 */
@Controller
public class MyController {


    @GetMapping("/index")
    public String index(){

        return "index";
    }
}
