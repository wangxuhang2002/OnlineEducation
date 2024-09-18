package com.atguigu.online.education.common.util;

import com.atguigu.online.education.common.constant.Constant;

public class SQLUtil {

    public static String getKafkaDDL(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getDorisDDL(String tableName){
        return "WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = '"+Constant.DORIS_FE_NODES+"', " +
                "  'table.identifier' = '"+Constant.DORIS_DATABASE+"."+tableName+"', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.enable-2pc' = 'false' " +
                ")  ";
    }
}
