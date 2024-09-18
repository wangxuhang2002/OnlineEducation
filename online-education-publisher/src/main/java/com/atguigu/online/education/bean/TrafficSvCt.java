package com.atguigu.online.education.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

// 流量域 会话数
@Data
@AllArgsConstructor
public class TrafficSvCt {
    // 来源
    String sc;
    // 会话数
    Integer svCt;
}