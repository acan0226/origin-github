package net.acan.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.annotation.NotSink;
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

    // ??????????????????, ???????????????????????????, ????????????sink
    public static <T> SinkFunction<T> getClickHouseSink(String db,
                                                               String table,
                                                               Class<T> tClass) {
        String driver = Constant.CLICKHOUSE_DRIVER;
        String url = Constant.CLICKHOUSE_PRE_URL + db;
        // ?????????????????????T?????????????????????????????????
        String fieldString = MyUtil.getFieldString(tClass);

        // ?????????????????????????????????sql??????
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
        System.out.println("clickhouse????????????: " + sql.toString());
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
                        // ??????class??????:  1. Class.forName(..)  2. ??????.class  3. ??????.getClass
                        // ???????????? t??????????????????, ???sql???????????????????????????
                        //TODO
                        // insert into abc(stt,edt,vc,ch,ar,is_new,uv_ct,pv_ct,sv_ct,uj_ct,dur_sum,ts)values(?,?,?,?,?,?,?,?,?,?,?,?)
                        Class<?> tClass = t.getClass();
                        try {
                        Field[] fields = tClass.getDeclaredFields();
                        for (int i = 0,p=1; i < fields.length; i++) {
                            Field field = fields[i];
                            NotSink noSink = field.getAnnotation(NotSink.class);
                            if (noSink == null) {
                                field.setAccessible(true);
                                Object v = field.get(t); // ??????????????????
                                ps.setObject(p++, v);  // ??????????????????
                            }



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
