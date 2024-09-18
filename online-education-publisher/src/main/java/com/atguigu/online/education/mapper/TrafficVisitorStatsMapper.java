package com.atguigu.online.education.mapper;

import com.atguigu.online.education.bean.TrafficVisitorStatsPerHour;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface TrafficVisitorStatsMapper {
    // 分时 流量数据 查询
    @Select("select hour(stt) hr,\n" +
            "       sum(uv_ct) uv_ct,\n" +
            "       sum(pv_ct) pv_ct,\n" +
            "       sum(pv_ct)/sum(sv_ct) pv_per_session\n" +
            "from dws_traffic_sc_vc_ch_ar_is_new_page_view_window\n" +
            "         partition (par#{date})\n" +
            "group by hr")
    List<TrafficVisitorStatsPerHour> selectVisitorStatsPerHr(@Param("date")Integer date);
}
