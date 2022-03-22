package net.acan.gmall.realtime.Function;



import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.JdbcUtil;
import net.acan.gmall.realtime.util.RedisUtil;
import net.acan.gmall.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author lzc
 * @Date 2022/3/22 9:47
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    
    public abstract void addDim(Jedis redisClient,
                                Connection conn,
                                T input,
                                ResultFuture<T> resultFuture) throws Exception;
    
    
    private ThreadPoolExecutor threadPool;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();
    }
    
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {
        // 每来一条数据, 就使用线程池启动一个线程进行读取维度信息
        threadPool.submit(new Runnable() {
            
            
            @Override
            public void run() {
                Connection conn = null;
                Jedis redisClient = null;
                // 读取维度的信息
                try {
                    conn = JdbcUtil.getJDBCConnection(Constant.PHONEIX_DRIVER, Constant.PHONEIX_URL, null, null);
                    redisClient = RedisUtil.getRedisClient();
                    
                    // 处理维度
                    // 将来不同流, T类型不同, 需要的维度表也不一样
                    addDim(redisClient, conn, input, resultFuture);
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // 关闭连接对象
                    //因为是在连接池中创建的连接，所有在finally中关闭
                    //如果在open中创建的连接，才要在close中关闭
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    
                    if (redisClient != null) {
                        redisClient.close();
                    }
                }
                
            }
        });
    }
}
