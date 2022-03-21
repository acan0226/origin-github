package net.acan.gmall.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private static JedisPool pool;
//靜態代碼塊，只執行一次，不用每次創建連接時都調用
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(10);
        config.setMaxWaitMillis(30 * 1000);
        config.setMinIdle(2);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        pool = new JedisPool(config,"hadoop162",6379);
    }

    public static Jedis getRedisClient(){
        //创建连接池
        Jedis jedis = pool.getResource();
        jedis.select(1);
        return jedis;
    }
}
