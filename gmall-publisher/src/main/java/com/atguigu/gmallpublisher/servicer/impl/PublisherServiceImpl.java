package com.atguigu.gmallpublisher.servicer.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.servicer.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: doubleZ
 * Datetime:2020/8/17   15:35
 * Description:
 */
//获取日活分时数据
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询Phoneix
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        //2.创建Map用于存调整结构后的数据
        HashMap<String, Long> result = new HashMap<>();
        //3.调整结构
        for (Map map : list) {
            result.put((String)map.get("LH"),(Long) map.get("CT"));
        }
        //4.返回数据
        return result;
    }
    public  Double getOrderAmountTotal(String date){
        return orderMapper.selectOrderAmountTotal(date);
    }

    public Map getOrderAmountHourMap(String date){
        //1.查询Phoenix
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        //2.创建Map用于存放结果数据
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }
}
