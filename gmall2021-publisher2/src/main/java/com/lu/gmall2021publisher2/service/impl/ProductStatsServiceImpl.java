package com.lu.gmall2021publisher2.service.impl;

import com.lu.gmall2021publisher2.mapper.ProductStatsMapper;
import com.lu.gmall2021publisher2.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.selectGmv(date);
    }

    @Override
    public Map getGmvByTm(int date, int limit) {
        //查询ClickHouse数据
        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);

        //创建Map存放结果
        HashMap<String, BigDecimal> result = new HashMap<>();

        //遍历maplist
        for (Map map : mapList) {
            result.put((String) map.get("tm_name"), (BigDecimal) map.get("order_amount"));
        }

        return result;
    }
}
