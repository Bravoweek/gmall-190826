package com.mchou.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mchou.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @GetMapping("test1")
    public String test1() {
        return "success";
    }

    @GetMapping("test2")
    public String test2(@RequestParam("aa") String aa) {
        return aa;
    }

    @PostMapping("log")
    public String logger(@RequestParam("logString") String logStr){

//        System.out.println(logStr);

        //0.添加时间戳字段
        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts",System.currentTimeMillis());

        String addtimestampleJson = jsonObject.toString();

        //1.使用log4j打印日志到控制台及文件
        log.info(addtimestampleJson);

        //使用kafka生产者将数据发送到控制台
        //1.先判断是什么类型数据
        if (addtimestampleJson.contains("startup")){
            kafkaTemplate.send(GmallConstants.GMALL_STARTUP_TOPIC,addtimestampleJson);
        }else {
            kafkaTemplate.send(GmallConstants.GMALL_EVENT_TOPIC,addtimestampleJson);
        }

        return "success";
    }





}

