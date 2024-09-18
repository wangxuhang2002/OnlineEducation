package com.atguigu.online.education.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

// 流量域 各会话页面访问时长
@Data
@AllArgsConstructor
public class TrafficDurPerSession {
    // 来源
    String sc;
    // 各会话页面访问时长
    BigDecimal durPerSession;
}