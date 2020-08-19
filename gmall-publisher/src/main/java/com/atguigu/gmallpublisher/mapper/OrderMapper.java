package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * Author: doubleZ
 * Datetime:2020/8/18   14:21
 * Description:
 */
public interface OrderMapper {
    //查询当日订单金额的分时统计
    public  Double selectOrderAmountTotal(String date);

    public List<Map> selectOrderAmountHourMap(String date);
}
