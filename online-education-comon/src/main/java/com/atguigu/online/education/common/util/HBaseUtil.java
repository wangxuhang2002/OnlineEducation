package com.atguigu.online.education.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class HBaseUtil {
    //获取HBase连接对象
    public static Connection getHBaseConnection() throws IOException {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "hadoop104,hadoop105,hadoop106");
            Connection connection = ConnectionFactory.createConnection(conf);
            return connection;
        }catch (Exception e){
            throw new RuntimeException();
        }
    }

    //关闭HBase连接对象
    public static void closeHBaseConnection(Connection connection){
        if (connection !=null || connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    //在hbase中创建表
    public static void createHBaseTable(Connection hbaseConn,String nameSpace,String tableName,String ... families) throws IOException {
        if (families.length<1){
            System.out.println("建表的时候至少需要指定一个列族");
            return;
        }
        try(Admin admin=hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println("表空间"+nameSpace+"下已经存在表"+tableName+"，请勿重复创建");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表"+tableName+"创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //删表
    public static void dropHBaseTable(Connection hbaseConn,String nameSpace,String tableName) throws IOException {
        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if (!admin.tableExists(tableNameObj)){
                System.out.println("表空间"+nameSpace+"下不存在表"+tableName+"，请勿删除不存在的表");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("表"+tableName+"删除成功");
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }
    public static void putRow(Connection hbaseConn, String nameSpace, String tableName, String family, String rowKey, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columnNames = jsonObj.keySet();
            for (String columnName : columnNames) {
                String columnValue = jsonObj.getString(columnName);
                if(columnValue != null){
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
                }
            }
            table.put(put);
            System.out.println("向表空间"+nameSpace+"下的表"+tableName+"中put数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    //从HBase表中删除数据
    public static void delRow(Connection hbaseConn, String nameSpace, String tableName,String rowKey){
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间"+nameSpace+"下的表"+tableName+"中删除数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static AsyncConnection getHBaseAsyncConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            AsyncConnection asyncHBaseConn = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncHBaseConn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //关闭异步操作HBase的连接对象
    public static void closeHBaseAsyncConnection(AsyncConnection asyncHBaseConn){
        if(asyncHBaseConn != null && !asyncHBaseConn.isClosed()){
            try {
                asyncHBaseConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static JSONObject readDimAsync(AsyncConnection asyncHBaseConn,String nameSpace,String tableName,String rowKey){
        try {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncHBaseConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                JSONObject dimJsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    dimJsonObj.put(columnName,columnValue);
                }
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }

}
