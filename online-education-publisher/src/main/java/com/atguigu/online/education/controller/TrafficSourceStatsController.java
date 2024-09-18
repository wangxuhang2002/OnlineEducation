package com.atguigu.online.education.controller;

import com.atguigu.online.education.bean.TrafficDurPerSession;
import com.atguigu.online.education.bean.TrafficPvPerSession;
import com.atguigu.online.education.bean.TrafficSvCt;
import com.atguigu.online.education.bean.TrafficUvCt;
import com.atguigu.online.education.service.TrafficSourceStatsService;
import com.atguigu.online.education.util.DateFormatUtil;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

// 流量域 来源统计 Controller
@RestController
@MapperScan("com.atguigu.online.education.mapper")
public class TrafficSourceStatsController {
    // 自动装载 流量域 来源统计服务类
    @Autowired
    TrafficSourceStatsService trafficSourceStatsService;
    // 拦截 某日 各来源 独立访客数 请求
    @RequestMapping("/sc/uvCt")
    public String getScUvCt(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        // 默认日期 设为 当日日期
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficSourceStatsService.getScUvCt(date);
        if (trafficUvCtList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder uvCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficUvCtList.size(); i++) {
            TrafficUvCt trafficUvCt = trafficUvCtList.get(i);
            String sc = trafficUvCt.getSc();
            Integer uvCt = trafficUvCt.getUvCt();

            categories.append("\"").append(sc).append("\"");
            uvCtValues.append("\"").append(uvCt).append("\"");

            if (i < trafficUvCtList.size() - 1) {
                categories.append(",");
                uvCtValues.append(",");
            } else {
                categories.append("]");
                uvCtValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"data\": " + uvCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }
    // 拦截 某日 各来源 绘画总数 请求
    @RequestMapping("/sc/svCt")
    public String getScSvCt(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        // 默认日期 设为 当日日期
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficSvCt> trafficSvCtList = trafficSourceStatsService.getScSvCt(date);
        if (trafficSvCtList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder svCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficSvCtList.size(); i++) {
            TrafficSvCt trafficSvCt = trafficSvCtList.get(i);
            String sc = trafficSvCt.getSc();
            Integer svCt = trafficSvCt.getSvCt();

            categories.append("\"").append(sc).append("\"");
            svCtValues.append("\"").append(svCt).append("\"");

            if (i < trafficSvCtList.size() - 1) {
                categories.append(",");
                svCtValues.append(",");
            } else {
                categories.append("]");
                svCtValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"绘画总数\",\n" +
                "        \"data\": " + svCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }
    // 拦截 某日 各来源 会话平均浏览页面数 请求
    @RequestMapping("/sc/pvPerSession")
    public String getScPvPerSession(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        // 默认日期 设为 当日日期
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficPvPerSession> trafficPvPerSessionList = trafficSourceStatsService.getScPvPerSession(date);
        if (trafficPvPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder pvPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficPvPerSessionList.size(); i++) {
            TrafficPvPerSession trafficPvPerSession = trafficPvPerSessionList.get(i);
            String sc = trafficPvPerSession.getSc();
            BigDecimal pvPerSession = trafficPvPerSession.getPvPerSession();

            categories.append("\"").append(sc).append("\"");
            pvPerSessionValues.append("\"").append(pvPerSession).append("\"");

            if (i < trafficPvPerSessionList.size() - 1) {
                categories.append(",");
                pvPerSessionValues.append(",");
            } else {
                categories.append("]");
                pvPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均浏览页面数\",\n" +
                "        \"data\": " + pvPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }
    // 拦截 某日 各来源 会话平均停留时长 请求
    @RequestMapping("/sc/durPerSession")
    public String getScDurPerSession(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        // 默认日期 设为 当日日期
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficDurPerSession> trafficDurPerSessionList = trafficSourceStatsService.getScDurPerSession(date);
        if (trafficDurPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder durPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficDurPerSessionList.size(); i++) {
            TrafficDurPerSession trafficUvCt = trafficDurPerSessionList.get(i);
            String sc = trafficUvCt.getSc();
            BigDecimal durPerSession = trafficUvCt.getDurPerSession();

            categories.append("\"").append(sc).append("\"");
            durPerSessionValues.append("\"").append(durPerSession).append("\"");

            if (i < trafficDurPerSessionList.size() - 1) {
                categories.append(",");
                durPerSessionValues.append(",");
            } else {
                categories.append("]");
                durPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均停留时长\",\n" +
                "        \"data\": " + durPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

}
