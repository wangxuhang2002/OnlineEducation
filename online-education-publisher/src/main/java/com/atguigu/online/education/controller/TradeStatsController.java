package com.atguigu.online.education.controller;

import com.atguigu.online.education.bean.TradeProvinceOrderAmount;
import com.atguigu.online.education.service.TradeStatsService;
import com.atguigu.online.education.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Felix
 * @date 2024/9/09
 * 交易域统计Controller
 * @Controller     将当前类对象的创建交给Spring容器，如果类中方式返回的是String，会进行页面的跳转
 *                 如果不想进行页面的跳转，可以在方法上加@ResponseBody
 * @RestController 将当前类对象的创建交给Spring容器，如果类中方式返回的是String，直接响应给客户端
 * @RequestMapping("/gmv")  拦截请求，将请求交给对应的方法进行处理
 */

@RestController
public class TradeStatsController {

    @Autowired
    TradeStatsService tradeStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            //请求中如果没有携带查询的日期，将当天作为查询日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        String json = "{\"status\": 0,\"data\": "+gmv+"}";
        return json;
    }

    /*@RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);
        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \""+provinceOrderAmount.getProvinceName()+"\",\"value\": "+provinceOrderAmount.getOrderAmount()+"}");
            if(i < provinceOrderAmountList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("],\"valueName\": \"交易额\"}}");

        return jsonB.toString();
    }*/

    @RequestMapping("/province")
    public Map getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);

        Map resMap = new HashMap();
        resMap.put("status",0);

        Map dataMap = new HashMap();
        List dataList = new ArrayList();
        for (TradeProvinceOrderAmount provinceOrderAmount : provinceOrderAmountList) {
            Map map = new HashMap();
            map.put("name", provinceOrderAmount.getProvinceName());
            map.put("value",provinceOrderAmount.getOrderAmount());
            dataList.add(map);
        }
        dataMap.put("mapData",dataList);
        dataMap.put("valueName","交易额");
        resMap.put("data",dataMap);
        return resMap;
    }
}
