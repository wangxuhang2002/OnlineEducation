package com.atguigu.online.education.service.impl;

import com.atguigu.online.education.bean.TrafficVisitorTypeStats;
import com.atguigu.online.education.mapper.TrafficVisitorTypeStatsMapper;
import com.atguigu.online.education.service.TrafficVisitorTypeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficVisitorTypeStatsServiceImpl implements TrafficVisitorTypeStatsService {
    @Autowired
    TrafficVisitorTypeStatsMapper trafficVisitorTypeStatsMapper;
    @Override
    public List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date) {
        return trafficVisitorTypeStatsMapper.selectVisitorTypeStats(date);
    }
}
