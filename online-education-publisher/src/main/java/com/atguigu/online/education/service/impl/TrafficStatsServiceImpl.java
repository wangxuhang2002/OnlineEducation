package com.atguigu.online.education.service.impl;

import com.atguigu.online.education.bean.TrafficDurPerSession;
import com.atguigu.online.education.bean.TrafficPvPerSession;
import com.atguigu.online.education.bean.TrafficSvCt;
import com.atguigu.online.education.service.TrafficStatsService;
import com.atguigu.online.education.bean.TrafficUvCt;
import com.atguigu.online.education.mapper.TrafficStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Felix
 * @date 2024/9/10
 * 流量域统计service接口实现类
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {

    @Autowired
    TrafficStatsMapper trafficStatsMapper;

    @Override
    public List<TrafficUvCt> getScUvCt(Integer date) {
        return trafficStatsMapper.selectScUvCt(date);
    }

    @Override
    public List<TrafficSvCt> getScSvCt(Integer date) {
        return trafficStatsMapper.selectScSvCt(date);
    }

    @Override
    public List<TrafficPvPerSession> getScPvPerSession(Integer date) {
        return trafficStatsMapper.selectScPvPerSession(date);
    }

    @Override
    public List<TrafficDurPerSession> getScDurPerSession(Integer date) {
        return trafficStatsMapper.selectScDurPerSession(date);
    }
}
