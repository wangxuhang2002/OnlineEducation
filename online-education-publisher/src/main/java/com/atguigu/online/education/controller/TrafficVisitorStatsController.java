package com.atguigu.online.education.controller;

import com.atguigu.online.education.bean.TrafficVisitorStatsPerHour;
import com.atguigu.online.education.service.TrafficVisitorStatsService;
import com.atguigu.online.education.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

// 流量域 流量分时统计 Controller
@RestController
public class TrafficVisitorStatsController {
    // 自动装载 流量域 流量分时统计服务类
    @Autowired
    TrafficVisitorStatsService trafficVisitorStatsService;
    // 访客状态分时统计请求拦截方法
    @RequestMapping("/visitorPerHr")
    public String getVisitorPerHr(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficVisitorStatsPerHour> visitorPerHrStatsList = trafficVisitorStatsService.getVisitorPerHrStats(date);
        if (visitorPerHrStatsList == null || visitorPerHrStatsList.size() == 0) {
            return "";
        }

        TrafficVisitorStatsPerHour[] perHrArr = new TrafficVisitorStatsPerHour[24];
        for (TrafficVisitorStatsPerHour trafficVisitorStatsPerHour : visitorPerHrStatsList) {
            Integer hr = trafficVisitorStatsPerHour.getHr();
            perHrArr[hr] = trafficVisitorStatsPerHour;
        }

        String[] hrs = new String[24];
        Long[] uvArr = new Long[24];
        Long[] pvArr = new Long[24];
        Long[] pvPerSession = new Long[24];

        for (int hr = 0; hr < 24; hr++) {
            hrs[hr] = String.format("%02d", hr);
            TrafficVisitorStatsPerHour trafficVisitorStatsPerHour = perHrArr[hr];
            if (trafficVisitorStatsPerHour != null) {
                uvArr[hr] = trafficVisitorStatsPerHour.getUvCt();
                pvArr[hr] = trafficVisitorStatsPerHour.getPvCt();
                pvPerSession[hr] = trafficVisitorStatsPerHour.getPvPerSession();
            } else{
                uvArr[hr] = 0L;
                pvArr[hr] = 0L;
                pvPerSession[hr] = 0L;
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\n\"" +
                StringUtils.join(hrs, "\",\"") + "\"\n" +
                "    ],\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(uvArr, ",") + "\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"页面浏览数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(pvArr, ",") + "\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"新访客数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(pvPerSession, ",") + "\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }
}
