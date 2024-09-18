package com.atguigu.online.education.controller;

import com.atguigu.online.education.bean.TrafficVisitorTypeStats;
import com.atguigu.online.education.service.TrafficVisitorTypeStatsService;
import com.atguigu.online.education.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TrafficVisitorTypeStatsController {
    @Autowired
    TrafficVisitorTypeStatsService trafficVisitorTypeStatsService;
    @RequestMapping("/visitorPerType")
    public String getVisitorPerType(
    @RequestParam(value = "date", defaultValue = "1") Integer date) {

        if (date == 1) {
            date = DateFormatUtil.now();
        }

        List<TrafficVisitorTypeStats> visitorTypeStatsList = trafficVisitorTypeStatsService.getVisitorTypeStats(date);

        if (visitorTypeStatsList == null || visitorTypeStatsList.size() == 0) {
            return "";
        }

        TrafficVisitorTypeStats newVisitorStats = null;
        TrafficVisitorTypeStats oldVisitorStats = null;
        for (TrafficVisitorTypeStats visitorStats : visitorTypeStatsList) {
//            System.out.println(visitorStats);
            if ("1".equals(visitorStats.getIsNew())) {
                // 新访客
                newVisitorStats = visitorStats;
            } else {
                // 老访客
                oldVisitorStats = visitorStats;
            }
        }
        //拼接json字符串
        String json = "{\"status\":0,\"data\":{\"total\":5," +
                "\"columns\":[" +
                "{\"name\":\"类别\",\"id\":\"type\"}," +
                "{\"name\":\"新访客\",\"id\":\"new\"}," +
                "{\"name\":\"老访客\",\"id\":\"old\"}]," +
                "\"rows\":[" +
                "{\"type\":\"独立访客数(人)\",\"new\":" + newVisitorStats.getUvCt() + ",\"old\":" + oldVisitorStats.getUvCt() + "}," +
                "{\"type\":\"总访问页面数(次)\",\"new\":" + newVisitorStats.getPvCt() + ",\"old\":" + oldVisitorStats.getPvCt() + "}," +
                "{\"type\":\"平均在线时长(秒)\",\"new\":" + newVisitorStats.getAvgDurSum() + ",\"old\":" + oldVisitorStats.getAvgDurSum() + "}," +
                "{\"type\":\"平均访问页面数(人次)\",\"new\":" + newVisitorStats.getAvgPvCt() + ",\"old\":" + oldVisitorStats.getAvgPvCt() + "}]}}";

        return json;
    }
}
