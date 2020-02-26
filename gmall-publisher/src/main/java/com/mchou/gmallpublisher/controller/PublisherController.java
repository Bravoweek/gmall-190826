package com.mchou.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.mchou.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    //1.先写请求地址，获取日活数据
    //http://localhost:8070/realtime-total?date=2019-09-06
    @GetMapping("realtime-total")
    public String gettotal(@RequestParam("date") String date){

        //获取日活数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //新建map，存放获取的数据
        ArrayList<Map> result = new ArrayList<>();

        //添加日活信息
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        //添加新增用户信息,最后获取的数据时写死的
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value","233");

        //将日活数据及新增数据添加至集合
        result.add(dauMap);
        result.add(newMidMap);

        //将传出来的数据转成json格式
        return JSON.toJSONString(result);
    }

    //1.先写请求地址，获取小时数据
    //http://localhost:8070/realtime-hours?id=dau&date=2019-09-06
    @GetMapping("realtime-hours")
    public String getRealTimeHour (@RequestParam("id") String id,@RequestParam ("date") String date) throws ParseException {

        //创建集合用户存放查询结果
        HashMap<String, Map> result = new HashMap<>();

        //创建集合用户存放查询结果
        Map todayMap = publisherService.getDauTotalHourMap(date);

        //查询昨日数据 date:2020-02-18
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar instance = Calendar.getInstance();
        instance.setTime(dateFormat.parse(date));

        //将当天时间减一
        instance.add(Calendar.DAY_OF_MONTH,-1);
        String yesterday = dateFormat.format(new Date(instance.getTimeInMillis()));
        Map yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

        //将今天的以及昨天的数据存放至result
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        //将集合转换为字符串返回
        return JSON.toJSONString(result);
    }


}

