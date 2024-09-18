package com.atguigu.online.education.service;

import com.atguigu.online.education.bean.TrafficVisitorTypeStats;

import java.util.List;

public interface TrafficVisitorTypeStatsService {
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);
}
