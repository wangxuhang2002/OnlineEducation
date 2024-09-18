package com.atguigu.online.education.mapper;

import com.atguigu.online.education.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

// 流量域 统计 mapper 接口
@Mapper
public interface TrafficStatsMapper {
    //获取 某天 各来源 独立访客数
    @Select("select\n" +
            "    sc,\n" +
            "    sum(uv_ct) uv_ct\n" +
            "from dws_traffic_sc_vc_ch_ar_is_new_page_view_window\n" +
            "    partition (par#{date})\n" +
            "group by sc\n" +
            "order by uv_ct desc")
    List<TrafficUvCt> selectScUvCt(@Param("date") Integer date);


}
