package com.atguigu.online.education.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

// 流量域 各会话页面浏览数
@Data
@AllArgsConstructor
public class TrafficPvPerSession {
    // 来源
    String sc;
    // 各会话页面浏览数
    BigDecimal pvPerSession;
}