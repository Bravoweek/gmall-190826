package com.mchou.gmallpublisher.service.impl;

import com.mchou.gmallpublisher.mapper.DauMapper;
import com.mchou.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceimpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //定义map，存储分时统计的数据
        HashMap<String,Long> hourDauMap = new HashMap<>();

        //查询Phoenix获取的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //遍历list:map((LogHour->13),(ct->183))
        for (Map map : list) {
            hourDauMap.put((String) map.get("LH"),(Long) map.get("CT"));
        }

        return hourDauMap;
    }


}
