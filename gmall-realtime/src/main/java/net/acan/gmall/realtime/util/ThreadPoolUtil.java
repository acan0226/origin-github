package net.acan.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/3/22 9:12
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(
            300, // 线程池的核心线程数
            500,// 线程池的最大线程数
            30, // 表示 500-300 这200个最多空闲的时间
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100) // 阻塞队列总最多存储的线程数
        );
    }
}
