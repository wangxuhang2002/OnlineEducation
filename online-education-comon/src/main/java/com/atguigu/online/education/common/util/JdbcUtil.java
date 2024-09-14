package com.atguigu.online.education.common.util;

import com.atguigu.online.education.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    // 获取 mysql 连接
    public static Connection getMySqlConnection() throws Exception {
        Class.forName(Constant.MYSQL_DRIVER);
        Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        return conn;
    }
    // 关闭 mysql 连接
    public static void closeMySqlConnection(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
    // 从 mysql 中，查询 数据，不转换 字段 名字
    public static <T>List<T> queryList(Connection connection, String sql, Class<T> clz) throws Exception {
        return queryList(connection, sql, clz, false);
    }
    // 从 mysql 中，查询 数据,蛇形命名 转为 驼峰命名
    public static <T>List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underLineToCamel) throws Exception {
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()){
            T obj = clz.newInstance();
            for (int i = 1; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = resultSet.getObject(i);
                if (underLineToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(obj, columnName, columnValue);
            }
            resultList.add(obj);
        }
        resultSet.close();
        ps.close();
        return resultList;
    }
}
