package com.atguigu.online.education.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficVisitorStatsPerHour {
    // 小时
    Integer hr;
    // 独立访客数
    Long uvCt;
    // 页面浏览数
    Long pvCt;
    // 会话平均页面浏览数
    Long pvPerSession;
}