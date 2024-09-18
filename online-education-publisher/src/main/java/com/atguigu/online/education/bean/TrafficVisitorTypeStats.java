package com.atguigu.online.education.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Data
@AllArgsConstructor
public class TrafficVisitorTypeStats {
    // 新老访客状态标记
    String isNew;
    // 独立访客数
    Long uvCt;
    // 页面浏览数
    Long pvCt;
    // 累计访问时长
    Long durSum;
    // 平均在线时长
    public BigDecimal getAvgDurSum() {
        if (uvCt == 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(durSum)
                .divide(BigDecimal.valueOf(uvCt), 3, RoundingMode.HALF_UP)
                .divide(BigDecimal.valueOf(1000L), 3, RoundingMode.HALF_UP);
    }
    // 平均访问页面数
    public BigDecimal getAvgPvCt() {
        if (uvCt == 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(pvCt).divide(BigDecimal.valueOf(uvCt), 3, RoundingMode.HALF_UP);
    }
}