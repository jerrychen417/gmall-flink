package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static <T>List<T> queryData(Connection connection, String sql, Class<T> clz, Boolean toCamel) throws Exception {
        ArrayList<T> resultList = new ArrayList<>();

        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历resultSet, 将每行数据转换为T对象
        while (resultSet.next()) {
            //创建T对象
            T t = clz.newInstance();

            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);

                if (toCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                //给T对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            //将T对象存放至集合
            resultList.add(t);
        }
        resultSet.close();
        preparedStatement.close();
        return resultList;
    }

    public static void main(String[] args) throws Exception {
        //注册驱动
        DriverManager.registerDriver(new com.mysql.jdbc.Driver());

        //mysql的url格式：jdbc协议:子协议://主机名:端口号/要连接的数据库名
        String url = "jdbc:mysql://hadoop102:3306/gmall_flink_210726";
        String user = "root";
        String password = "123456";
        Connection connection = DriverManager.getConnection(url, user, password);

        List<JSONObject> jsonObjects = queryData(connection, "select * from base_province ;", JSONObject.class, false);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

        connection.close();
    }
}
