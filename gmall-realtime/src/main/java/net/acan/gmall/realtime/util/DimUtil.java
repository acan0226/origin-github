package net.acan.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    private static JSONObject dim;

    public static JSONObject readDimFromPhoenix(Connection phoenixConn,
                                                String table,
                                                Long id) throws Exception {
        String sql = "select * from "+ table + " where id = ?";
        //数组中存储所有占位符的值，占位符有几个，数组就有几个。
        String[] args = {id.toString()};
        //执行sql，把得到的结果封装到list集合返回

        List<JSONObject> list = JdbcUtil.queryList(phoenixConn,sql,args,JSONObject.class);

        return list.get(0);


    }

    public static JSONObject readDim(Jedis redisClient,
                                     Connection phoenixConn,
                                     String tableName,
                                     Long id) throws Exception {
    //1.先从redis中读
        dim = readDimFromRedis(redisClient,tableName,id);
//2.如果Redis中没读到，再从phoenix中读取
        if (dim == null) {//从缓存没读到维度数据
            dim = readDimFromPhoenix(phoenixConn,tableName,id);
            //3.在从phoenix中读，写入到redis中
            writeDimToRedis(redisClient,tableName,id,dim);
            System.out.println("走数据库: " + tableName + "  " + id);

        } else {
            System.out.println("走缓存: " + tableName + "  " + id);
        }


        return dim;
    }

    private static void writeDimToRedis(Jedis redisClient, String tableName, Long id, JSONObject dim) {
                /*
        string
         key:  表名:id
         value:  dim数据, json'格式
         */
        String key = tableName +":"+id;
//        redisClient.set(key, dim.toJSONString());
//        redisClient.expire(key,2 * 24 * 60 * 60);
        //这两行可以用这一行替换
        redisClient.setex(key, 2 * 24 * 60 * 60, dim.toJSONString());
    }

    private static JSONObject readDimFromRedis(Jedis redisClient, String tableName, Long id) {
        String key = tableName +":"+id;
        String dimJson = redisClient.get(key);
        if (dimJson != null) {// 缓存可能不存在
            return JSON.parseObject(dimJson);
        }
        return null;
    }


}
