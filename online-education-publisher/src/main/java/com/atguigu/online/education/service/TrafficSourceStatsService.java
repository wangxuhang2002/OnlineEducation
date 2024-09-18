package com.atguigu.online.education.service;

import com.atguigu.online.education.bean.TrafficDurPerSession;
import com.atguigu.online.education.bean.TrafficPvPerSession;
import com.atguigu.online.education.bean.TrafficSvCt;
import com.atguigu.online.education.bean.TrafficUvCt;

import java.util.List;

/**
 * @author Felix
 * @date 2024/9/10
 * 流量域统计service接口
 */
public interface TrafficSourceStatsService {
    // 获取 某天 各来源 独立访客数
    List<TrafficUvCt> getScUvCt(Integer date);
    // 获取 某天 各来源 会话数
    List<TrafficSvCt> getScSvCt(Integer date);
    // 获取 某天 各来源 会话平均页面浏览数
    List<TrafficPvPerSession> getScPvPerSession(Integer date);
    // 获取 某天 各来源 会话平均页面访问时长
    List<TrafficDurPerSession> getScDurPerSession(Integer date);
}
