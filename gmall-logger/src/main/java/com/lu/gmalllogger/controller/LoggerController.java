package com.lu.gmalllogger.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping
    public String test1() {
        System.out.println("111111111");
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        //落盘
        Logger logger = LoggerFactory.getLogger(LoggerController.class);
        logger.info(jsonStr);

        //将数据写入kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}
