package com.atguigu.online.education.dws.app;

import com.atguigu.online.education.common.base.BaseSQLApp;
import com.atguigu.online.education.common.constant.Constant;
import com.atguigu.online.education.common.util.SQLUtil;
import com.atguigu.online.education.dws.function.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // 1. 注册 函数
        tableEnv.createTemporarySystemFunction("split_word", KeywordUDTF.class);
        // 2. 创建 动态表
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "    `common` map<string, string>,\n" +
                "    `page` map<string, string>,\n" +
                "    `ts` bigint,\n" +
                "    `et` as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "    WATERMARK FOR et AS et\n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));

        Table searchTable = tableEnv.sqlQuery("select \n" +
                "    page['item'] full_word,\n" +
                "    et\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'\n" +
                "and page['item'] is not null");

        tableEnv.createTemporaryView("search_table", searchTable);

        Table splitTable = tableEnv.sqlQuery("select\n" +
                "    keyword,\n" +
                "    et\n" +
                "from search_table, LATERAL TABLE(split_word(full_word)) t(keyword)");

        tableEnv.createTemporaryView("split_table", splitTable);

        Table resTable = tableEnv.sqlQuery("SELECT\n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "    keyword,\n" +
                "    SUM(*) keywrod_count\n" +
                "FROM TABLE(TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "GROUP BY keyword, window_start, window_end");

        tableEnv.executeSql("create table "+Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW+"(\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                ")" + SQLUtil.getDorisDDL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));

        resTable.executeInsert(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);
    }
}
