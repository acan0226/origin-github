package net.acan.gmall.realtime.app;

import net.acan.gmall.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public abstract class BaseAppV2 {
    public void init(int port,int p,String ck,String groupId,String firstTopic,String ... otherTopics){
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        //开启checkpoint
        env.enableCheckpointing(3000);//每3秒开启一次checkpoint
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck/gmall/"+ck);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 存储了所有topic
        ArrayList<String> topics = new ArrayList<>();
        topics.add(firstTopic);
        topics.addAll(Arrays.asList(otherTopics));

        // 创建一个HashMap存储 topic->stream , key是topic名，value是stream
        HashMap<String, DataStreamSource<String>> topicStreamMap = new HashMap<>();
        for (String topic : topics) {
            DataStreamSource<String> stream = env
                    .addSource(FlinkSourceUtil.getKafkaSource(groupId, topic));
            topicStreamMap.put(topic,stream);

        }


       //TODO
        handle(env,topicStreamMap);


        try {
            env.execute(groupId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract void handle(StreamExecutionEnvironment env,
                                HashMap<String, DataStreamSource<String>> topicStreamMap);

}
