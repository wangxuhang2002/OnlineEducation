package com.atguigu.online.education.service;

import com.atguigu.online.education.bean.TrafficUvCt;

import java.util.List;

/**
 * @author Felix
 * @date 2024/9/10
 * 流量域统计service接口
 */
public interface TrafficStatsService {
    // 获取 某天 各来源 独立访客数
    List<TrafficUvCt> getScUvCt(Integer date);
}
