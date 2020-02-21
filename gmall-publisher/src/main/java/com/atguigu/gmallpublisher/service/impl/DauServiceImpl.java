package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {
    @Autowired
    DauMapper dauMapper;

    @Override
    public int getTotal(String date) {
        return dauMapper.getTotal(date);
    }

    @Override
    public Map getRealTimeHours(String date) {

        //查询的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建Map存放数据结构
        HashMap<String, Long> map = new HashMap<>();

        //遍历查询到的数据
        for (Map map1 : list) {
            //强转
        map.put((String)map1.get("LH"),(Long)map1.get("CT"));

        }

        return map;
    }
}
