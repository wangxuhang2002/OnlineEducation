package com.atguigu.online.education.service;

import com.atguigu.online.education.bean.TrafficVisitorStatsPerHour;

import java.util.List;

public interface TrafficVisitorStatsService {
    // 获取 分时 流量数据
    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);
}
