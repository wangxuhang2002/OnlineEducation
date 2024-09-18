package com.atguigu.online.education.service.impl;

import com.atguigu.online.education.bean.TrafficVisitorStatsPerHour;
import com.atguigu.online.education.mapper.TrafficVisitorStatsMapper;
import com.atguigu.online.education.service.TrafficVisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficVisitorStatsServiceImpl implements TrafficVisitorStatsService {

    @Autowired
    private TrafficVisitorStatsMapper trafficVisitorStatsMapper;

    // 获取 分时 流量 统计数据
    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorStatsPerHr(date);
    }
}
