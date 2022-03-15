package net.acan.gmall.realtime.util;

import net.acan.gmall.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkSourceUtil {
    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        Properties pros = new Properties();
        pros.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        pros.put("group.id",groupId);
        pros.put("auto.offset.reset","latest");
        pros.put("isolation.level","read_committed");

        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                pros);
    }
}
