package com.atguigu.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }
    //value:{"database":"gmall-210726-flink","before":{},"after":{"tm_name":"shanghai","id":17},"type":"insert","tableName":"base_trademark","sinkTable":"dim_base_trademark"}

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        //1.准备SQL语句:upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
        String sinkTable = value.getString("sinkTable");
        JSONObject after = value.getJSONObject("after");
        String sql = genSql(sinkTable, after);

        System.out.println("phoeni write>>>: " + sql);

        //2.预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //如果当前为更新操作,则先删除Redis数据
        if ("update".equals(value.getString("type"))) {
            DimUtil.delDimInfo(sinkTable.toUpperCase(), after.getString("id"));
        }

        //3.执行写入
        preparedStatement.execute();
        connection.commit();

        //4.释放资源
        preparedStatement.close();
    }

    //upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
    private String genSql(String sinkTable, JSONObject after) {
        Set<String> columns = after.keySet();
        Collection<Object> values = after.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + StringUtils.join(columns, ",") + ")" + "values('" + StringUtils.join(values, "','") + "')";
    }
}
