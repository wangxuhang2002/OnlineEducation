package com.atguigu.online.education.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

// 流量域 独立访客数
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 来源
    String sc;
    // 独立访客数
    Integer uvCt;
}

