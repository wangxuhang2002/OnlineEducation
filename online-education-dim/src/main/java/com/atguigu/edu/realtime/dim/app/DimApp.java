package com.atguigu.edu.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.dim.function.DimSinkFunction;
import com.atguigu.edu.realtime.dim.function.TableProcessFunction;
import com.atguigu.online.education.common.base.BaseAPP;
import com.atguigu.online.education.common.bean.TableProcessDim;
import com.atguigu.online.education.common.constant.Constant;
import com.atguigu.online.education.common.util.FlinkSourceUtil;
import com.atguigu.online.education.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;


public class DimApp extends BaseAPP {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10002,4,"dim_app","topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换并进行简单的ETL    jsonStr--->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");
                        if ("edu".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );
//        jsonObjDS.print();

        //TODO 2.使用FlinkCDC读取配置表信息
        //2.1 创建MySqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("edu_config", "table_process_dim");
        //2.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS
                = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        //2.3 对配置数据进行转换  jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //为了处理方便，将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取对配置表进行的操作的类型
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            //说明从配置表删除了一条数据  从before属性中获取配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            //说明从对配置表进行了读取、新增、修改操作  从after属性中获取配置信息
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        tpDS.print();

        //TODO 3.根据配置表中的信息到HBase中建表或删表
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hBaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tableProcessDim.getOp();

                        String sinkTable = tableProcessDim.getSinkTable();
                        String[] families = tableProcessDim.getSinkFamily().split(",");
                        if ("c".equals(op) || "r".equals(op)) {
                            //从配置表中读取或者向配置表中新增了数据  在Hbase中执行建表操作
                            HBaseUtil.createHBaseTable(hBaseConn, Constant.HBASE_NAMESPACE, sinkTable, families);
                        } else if ("d".equals(op)) {
                            //从配置表中删除了一条配置信息   将HBase中对应的表也删除掉
                            HBaseUtil.dropHBaseTable(hBaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else {
                            //对配置表的信息进行了更新操作
                            // 先从Hbase删除表
                            HBaseUtil.dropHBaseTable(hBaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            // 再新建表
                            HBaseUtil.createHBaseTable(hBaseConn, Constant.HBASE_NAMESPACE, sinkTable, families);

                        }
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();

        //TODO 4.将配置流进行广播---broadcast  将主流和广播流进行关联---connect 对关联后的数据进行处理---process
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        dimDS.print();

        //TODO 5.将维度数据写到HBase表中
        dimDS.print();
        dimDS.addSink(new DimSinkFunction());


    }
}