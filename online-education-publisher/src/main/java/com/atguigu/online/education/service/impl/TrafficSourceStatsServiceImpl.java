package com.atguigu.online.education.service.impl;

import com.atguigu.online.education.bean.TrafficDurPerSession;
import com.atguigu.online.education.bean.TrafficPvPerSession;
import com.atguigu.online.education.bean.TrafficSvCt;
import com.atguigu.online.education.service.TrafficSourceStatsService;
import com.atguigu.online.education.bean.TrafficUvCt;
import com.atguigu.online.education.mapper.TrafficSourceStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class TrafficSourceStatsServiceImpl implements TrafficSourceStatsService {

    @Autowired
    TrafficSourceStatsMapper trafficSourceStatsMapper;

    @Override
    public List<TrafficUvCt> getScUvCt(Integer date) {
        return trafficSourceStatsMapper.selectScUvCt(date);
    }

    @Override
    public List<TrafficSvCt> getScSvCt(Integer date) {
        return trafficSourceStatsMapper.selectScSvCt(date);
    }

    @Override
    public List<TrafficPvPerSession> getScPvPerSession(Integer date) {
        return trafficSourceStatsMapper.selectScPvPerSession(date);
    }

    @Override
    public List<TrafficDurPerSession> getScDurPerSession(Integer date) {
        return trafficSourceStatsMapper.selectScDurPerSession(date);
    }

    @Override
    public List<TrafficUvCt> getChapterUvCt(Integer date) {
        return trafficSourceStatsMapper.selectChapterUvCt(date);
    }
}
