package net.acan.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.bean.TableProcess;
import net.acan.gmall.realtime.bean.VisitorStats;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class FlinkSinkUtil {
    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<String>(
                "default",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                    @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element,
                                                                    @Nullable Long timestamp) {
                        String topic = element.f1.getSink_table();
                        String data = element.f0.toJSONString();
                        return new ProducerRecord<>(topic, data.getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE

        );
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
            return new PhoenixSink();
    }

    // 根据数据和表, 还有流中的数据类型, 返回一个sink
    public static <T> SinkFunction<T> getClickHouseSink(String db,
                                                               String table,
                                                               Class<T> tClass) {
        String driver = Constant.CLICKHOUSE_DRIVER;
        String url = Constant.CLICKHOUSE_PRE_URL + db;
        // 表中的字段要和T类型中的属性名保持一致
        String fieldString = MyUtil.getFieldString(tClass);

        // 需要实现一个插入数据的sql语句
        // insert into t(id, age, name)values(?,?,?)
        StringBuilder sql = new StringBuilder();
        sql
                .append("insert into ")
                .append(table)
                .append("(")
                .append(fieldString)
                .append(")values(")
                .append(fieldString.replaceAll("[^,]+", "?"))
                .append(")");
        System.out.println("clickhouse插入语句: " + sql.toString());
        return getJdbcSink(driver,url,null,null,sql.toString());
    }

    public static void main(String[] args) {
        getClickHouseSink("","aaa", VisitorStats.class);
    }

    private static <T> SinkFunction<T> getJdbcSink(String driver,
                                                   String url,
                                                   String user,
                                                   String password,
                                                   String sql) {

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps,
                                       T t) throws SQLException {
                        // 获取class方式:  1. Class.forName(..)  2. 类名.class  3. 对象.getClass
                        // 利用对象 t中的属性的值, 给sql中的占位符进行赋值
                        Class<?> tClass = t.getClass();
                        try {
                        Field[] fields = tClass.getDeclaredFields();
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            field.setAccessible(true);

                            Object  v = field.get(t);

                            ps.setObject(i+1,v);

                        } } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchIntervalMs(3000)
                        .withBatchSize(1024)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(driver)
                        .withUrl(url)
                        .withUsername(user)
                        .withPassword(password)
                        .build());
    }
}
