package com.atguigu.online.education.mapper;

import com.atguigu.online.education.bean.TrafficVisitorTypeStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface TrafficVisitorTypeStatsMapper {
    @Select("select is_new,\n" +
            "       sum(uv_ct)   uv_ct,\n" +
            "       sum(pv_ct)   pv_ct,\n" +
            "       sum(sv_ct)   sv_ct,\n" +
            "       sum(dur_sum) dur_sum\n" +
            "from dws_traffic_sc_vc_ch_ar_is_new_page_view_window\n" +
            "         partition (par#{date})\n" +
            "group by is_new")
    List<TrafficVisitorTypeStats> selectVisitorTypeStats(@Param("date")Integer date);
}
