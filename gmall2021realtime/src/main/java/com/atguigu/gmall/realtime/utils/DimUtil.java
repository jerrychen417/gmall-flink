package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String table, String key) throws Exception {

        //todo 添加旁路缓存
        //todo 查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;
        String jsonstr = jedis.get(redisKey);

        if (jsonstr != null) {
            jedis.expire(redisKey, 24 * 60 *60);
            jedis.close();
            return JSON.parseObject(jsonstr);
        }

        //拼接SQL
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + table + " where id='" + key + "'";

        System.out.println("查询SQL为:" + sql);

        //查询数据
        List<JSONObject> list = JdbcUtil.queryData(connection, sql, JSONObject.class, false);


        //todo 更新Redis数据
        JSONObject dimInfo = list.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        return dimInfo;
    }

    public static void delDimInfo(String table, String key) {
        String redisKey = "DIM:" + table + ":" + key;
        Jedis jedis = RedisUtil.getJedis();

        jedis.del(redisKey);

        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_CATEGORY1", "18"));
        long end = System.currentTimeMillis();

        System.out.println(getDimInfo(connection, "DIM_BASE_CATEGORY1", "18"));
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);  //10

        connection.close();
    }
}
