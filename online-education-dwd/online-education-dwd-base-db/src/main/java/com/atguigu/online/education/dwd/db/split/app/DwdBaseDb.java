package com.atguigu.online.education.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.online.education.common.base.BaseAPP;
import com.atguigu.online.education.common.bean.TableProcessDwd;
import com.atguigu.online.education.common.constant.Constant;
import com.atguigu.online.education.common.util.FlinkSinkUtil;
import com.atguigu.online.education.common.util.FlinkSourceUtil;
import com.atguigu.online.education.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

public class DwdBaseDb extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseDb().start(
                10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 1. 类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的json");
                        }
                    }
                }
        );
        // 2. 使用 FlinkCDC,从 mysql 中 读取 配置信息
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("edu_config", "table_process_dwd");
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");

        mysqlStrDS.print();
        // 3. 配置信息数据 类型转换 转换为 实体类
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDwd tableProcessDwd = null;
                        if ("d".equals(op)) {
                            tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class);
                        } else {
                            tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tableProcessDwd.setOp(op);
                        return tableProcessDwd;
                    }
                }
        );
        // 4. 将 配置信息数据流 转为 广播流
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        // 5. 将 非广播流 和 广播流 关联
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        // 6. 关联数据的
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> realDS = connectDS.process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {

//            private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;

            private Map<String, TableProcessDwd> configMap = new HashMap<>();

            // 预加载 配置信息
            @Override
            public void open(Configuration parameters) throws Exception {
                Connection mySqlConnection = JdbcUtil.getMySqlConnection();
                String sql = "select * from edu_config.table_process_dwd";
                List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mySqlConnection, sql, TableProcessDwd.class, true);
                for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                    String sourceTable = tableProcessDwd.getSourceTable();
                    String sourceType = tableProcessDwd.getSourceType();
                    String key = getKey(sourceTable, sourceType);
                    configMap.put(key, tableProcessDwd);
                }
                JdbcUtil.closeMySqlConnection(mySqlConnection);
            }

            @Override
            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                String table = jsonObj.getString("table");
                String type = jsonObj.getString("type");
                String key = getKey(table, type);
                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                TableProcessDwd tableProcessDwd = null;
                if ((tableProcessDwd = broadcastState.get(key)) != null
                        || (tableProcessDwd = configMap.get(key)) != null) {
                    JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                    String sinkColumns = tableProcessDwd.getSinkColumns();
                    deleteNotNeedColumns(dataJsonObj, sinkColumns);
                    dataJsonObj.put("ts", jsonObj.getLong("ts"));
                    out.collect(Tuple2.of(dataJsonObj, tableProcessDwd));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                String op = tableProcessDwd.getOp();
                //获取广播状态
                BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String sourceTable = tableProcessDwd.getSourceTable();
                String sourceType = tableProcessDwd.getSourceType();
                String key = getKey(sourceTable, sourceType);
                if("d".equals(op)){
                    //从配置表中删除了一条配置信息    从广播状态以及ConfigMap中删除对应的配置
                    broadcastState.remove(key);
                    configMap.remove(key);
                }else {
                    //对配置表进行了读取、添加以及更新操作  将最新的信息放到广播状态以及ConfigMap中
                    broadcastState.put(key,tableProcessDwd);
                    configMap.put(key,tableProcessDwd);
                }
            }

            private void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
                List<String> columnList = Arrays.asList(sinkColumns.split(","));
                Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
                entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
            }

            private String getKey(String sourceTable, String sourceType) {
                return sourceTable + ":" + sourceType;
            }
        });

//        realDS.print();

        realDS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
