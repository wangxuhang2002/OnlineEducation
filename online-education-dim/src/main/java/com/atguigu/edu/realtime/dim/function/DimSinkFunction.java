package com.atguigu.edu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.online.education.common.bean.TableProcessDim;
import com.atguigu.online.education.common.constant.Constant;
import com.atguigu.online.education.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;


public class DimSinkFunction  extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection hBaseConn;
    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hBaseConn);
    }

    //将流中数据同步到HBase表中
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup2, Context context) throws Exception {
        JSONObject jsonObj = tup2.f0;
        TableProcessDim tableProcessDim = tup2.f1;
        //获取对业务数据库维度表进行的操作类型
        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        //获取操作的HBase表的表名
        String sinkTable = tableProcessDim.getSinkTable();
        //获取rowkey的值
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());

        if("delete".equals(type)){
            //说明在业务数据库中从维度表中删除了一条数据     对应的从HBase表中删除一条数据
            HBaseUtil.delRow(hBaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else{
            //说明在业务数据库中对维度表进行了 insert update bootstrap-insert     对应的在HBase表中执行put操作
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamily,rowKey,jsonObj);
        }
    }
}
