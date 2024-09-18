package com.atguigu.online.education.dwd.db.app;

import com.atguigu.online.education.common.base.BaseSQLApp;
import com.atguigu.online.education.common.constant.Constant;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // 1.从 kafka topic_db主题中 读取数据 ，创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //tableEnv.executeSql("select * from topic_db").print();

        //TODO 2.过滤出加购行为
        Table cartInfoTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['course_id'] course_id,\n" +
                "    ts\n" +
                "from topic_db \n" +
                "    where \n" +
                "        `table`='cart_info'\n" +
                "    and (\n" +
                "        `type`='insert' \n" +
                "    )");
        //cartInfoTable.execute().print();

        //TODO 3.将加购数据写到kafka主题中
        //3.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_TRADE_CART_ADD+" (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+Constant.TOPIC_DWD_TRADE_CART_ADD+"',\n" +
                "  'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        //3.2 写入
        cartInfoTable.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
