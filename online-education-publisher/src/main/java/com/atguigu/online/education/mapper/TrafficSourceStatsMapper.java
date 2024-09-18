package com.atguigu.online.education.mapper;

import com.atguigu.online.education.bean.TrafficDurPerSession;
import com.atguigu.online.education.bean.TrafficPvPerSession;
import com.atguigu.online.education.bean.TrafficSvCt;
import com.atguigu.online.education.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

// 流量域 统计 mapper 接口
@Mapper
public interface TrafficSourceStatsMapper {
    // 获取 某天 各来源 独立访客数
    @Select("select\n" +
            "    sc,\n" +
            "    sum(uv_ct) uv_ct\n" +
            "from dws_traffic_sc_vc_ch_ar_is_new_page_view_window\n" +
            "    partition (par#{date})\n" +
            "group by sc\n" +
            "order by uv_ct desc")
    List<TrafficUvCt> selectScUvCt(@Param("date") Integer date);
    // 获取 某天 各来源 会话数
    @Select("select\n" +
            "    sc,\n" +
            "    sum(sv_ct) sv_ct\n" +
            "from dws_traffic_sc_vc_ch_ar_is_new_page_view_window\n" +
            "    partition (par#{date})\n" +
            "group by sc\n" +
            "order by sv_ct desc")
    List<TrafficSvCt> selectScSvCt(@Param("date") Integer date);
    // 获取 某天 各来源 会话平均页面浏览数
    @Select("select\n" +
            "    sc,\n" +
            "    sum(pv_ct)/sum(sv_ct) pv_per_session\n" +
            "from dws_traffic_sc_vc_ch_ar_is_new_page_view_window\n" +
            "    partition (par#{date})\n" +
            "group by sc\n" +
            "order by pv_per_session desc")
    List<TrafficPvPerSession> selectScPvPerSession(@Param("date") Integer date);
    // 获取 某天 各来源 会话平均页面访问时长
    @Select("select\n" +
            "    sc,\n" +
            "    sum(dur_sum)/sum(sv_ct) dur_per_session\n" +
            "from dws_traffic_sc_vc_ch_ar_is_new_page_view_window\n" +
            "    partition (par#{date})\n" +
            "group by sc\n" +
            "order by dur_per_session desc")
    List<TrafficDurPerSession> selectScDurPerSession(@Param("date") Integer date);

}
