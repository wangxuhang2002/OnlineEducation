package com.atguigu.edu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.online.education.common.bean.TableProcessDim;
import com.atguigu.online.education.common.constant.Constant;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;


public class TableProcessFunction  extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private Map<String,TableProcessDim> configMap = new HashMap<>();
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //~~~将配置表中的配置信息预加载到程序中~~~
        //注册驱动
        Class.forName(Constant.MYSQL_DRIVER);
        //获取连接
        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        //创建数据库操作对象
        String sql = "select * from edu_config.table_process_dim";
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行sql语句
        ResultSet rs = ps.executeQuery();
        //处理结果集
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            //定义一个json对象 用于封装查询出来的一条结果
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObj.put(columnName,columnValue);
            }
            //将json转换为实体类对象
            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }

        //释放资源
        rs.close();
        ps.close();
        conn.close();
    }

    //processElement:处理主流业务数据           根据广播状态中的配置信息判断当前处理的数据是不是维度数据
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //获取业务数据库中的表名
        String key = jsonObj.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据表名到广播状态中获取对应的配置信息
        TableProcessDim tableProcessDim = null;


        if((tableProcessDim = broadcastState.get(key)) != null
                ||(tableProcessDim = configMap.get(key)) != null){
            //如果从广播状态中获取的配置信息不为空，说明处理的是维度 将其中data部分以及对应的配置封装为二元组 发送到下游
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //在向下游传递数据前，添加type属性
            String type = jsonObj.getString("type");
            dataJsonObj.put("type",type);
            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }

    }

    //processBroadcastElement:处理广播流数据    将配置信息放到广播状态中
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取对配置表进行的操作的类型
        String op = tableProcessDim.getOp();
        String key = tableProcessDim.getSourceTable();
        if("d".equals(op)){
            //从配置表中删除了一条配置信息   从广播状态中删除对应的配置
            broadcastState.remove(key);
            configMap.remove(key);
        }else {
            //从配置表中读取一条数据或者向配置表中添加一条数据或者更新了一条配置信息  将最新的配置更新到广播状态中
            broadcastState.put(key,tableProcessDim);
            configMap.put(key,tableProcessDim);
        }
    }
}
