package com.atguigu.online.education.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool jedisPool;
    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMinIdle(5);
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig,"hadoop102",6379,10000);
    }
    //获取Jedis
    public static Jedis getJedis(){
        System.out.println("~~~获取Jedis~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
    //关闭Jedis
    public static void closeJedis(Jedis jedis){
        System.out.println("~~~关闭Jedis~~~");
        if(jedis != null){
            jedis.close();
        }
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        closeJedis(jedis);
    }

    //从Redis中读取维度数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        //拼接查询Redis的key
        String key = getKey(tableName,id);
        //根据key到Redis中获取维度数据
        String dimJsonStr = jedis.get(key);
        if(StringUtils.isNotEmpty(dimJsonStr)){
            //缓存命中
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }
    //向Redis中写入维度数据
    public static void writeDim(Jedis jedis,String tableName,String id,JSONObject dimJsonObj){
        //拼接key
        String key = getKey(tableName, id);
        jedis.setex(key,24*3600,dimJsonObj.toJSONString());
    }

    public static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    //获取异步操作Redis的客户端对象
    public static StatefulRedisConnection<String,String> getRedisAsyncConnection(){
        System.out.println("~~~获取异步Redis客户端~~~");
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
        StatefulRedisConnection<String, String> asyncRedisConn = redisClient.connect();
        return asyncRedisConn;
    }
    //关闭异步操作Redis的客户端对象
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConn){
        System.out.println("~~~关闭异步Redis客户端~~~");
        if(asyncRedisConn != null && asyncRedisConn.isOpen()){
            asyncRedisConn.close();
        }
    }
    //以异步的方式从Redis中读取维度数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> asyncRedisConns,String tableName,String id){
        //拼接查询Redis的key
        String key = getKey(tableName,id);
        //根据key到Redis中获取维度数据
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConns.async();

        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if(StringUtils.isNotEmpty(dimJsonStr)){
                //缓存命中
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }
    //以异步的方式向Redis中写入维度数据
    public static void writeDimAsync(StatefulRedisConnection<String, String> asyncRedisConns,String tableName,String id,JSONObject dimJsonObj){
        //拼接key
        String key = getKey(tableName, id);
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConns.async();
        asyncCommands.setex(key,24*3600,dimJsonObj.toJSONString());
    }

}
