package com.atguigu.online.education.controller;

import com.atguigu.online.education.bean.TrafficUvCt;
import com.atguigu.online.education.service.TrafficStatsService;
import com.atguigu.online.education.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author Felix
 * @date 2024/9/10
 * 流量域统计Controller
 */
@RestController
public class TrafficStatsController {
    // 自动装载 流量域 统计服务类
    @Autowired
    TrafficStatsService trafficStatsService;
    // 拦截 某日 各来源 独立访客数 请求
    @RequestMapping("/sc/uvCt")
    public String getScUvCt(@RequestParam(value = "date", defaultValue = "1") Integer date) {
        // 默认日期 设为 当日日期
        if (date == 1) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficStatsService.getScUvCt(date);
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

}
