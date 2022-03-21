package net.acan.gmall.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.bean.TableProcess;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.JdbcUtil;
import net.acan.gmall.realtime.util.RedisUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection conn;
    private ValueState<Boolean> tableCreateState;
    private Jedis redisClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取phoenix连接对象  一个并行度一个连接对象
        conn = JdbcUtil.getJDBCConnection(Constant.PHONEIX_DRIVER,
                Constant.PHONEIX_URL,
                null,
                null
        );
        tableCreateState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<Boolean>("tableCreateState", Boolean.class));

        redisClient = RedisUtil.getRedisClient();
    }

    @Override
    public void close() throws Exception {
        //关闭phoenix连接对象
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value,
                       Context context) throws Exception {
        //1.建表
        checktable(value);
        //2.将这条数据写入到对应的表中
        writeToPhoenix(value);
        //3.更新缓存
        updateCache(value);

    }

    private void updateCache(Tuple2<JSONObject, TableProcess> value) {
        // 1. 优雅
        // 1.1 如果存在就更新, 不存在, 就不用操作
        JSONObject dim = value.f0;
        TableProcess tp = value.f1;
        // key: table:id
        String key = tp.getSink_table()+":"+dim.getLong("id");
        if (redisClient.exists(key)) {
            // 先把字段名变成大写之后, 再去更新
            JSONObject upperDim = new JSONObject();
            for (Map.Entry<String, Object> entry : dim.entrySet()) {
                upperDim.put(entry.getKey().toUpperCase(),entry.getValue());
            }
            redisClient.setex(key, 2 * 24 * 60 * 60, upperDim.toJSONString());
        }

        // 2. 粗暴
        // 直接把缓存中的数据删除. 将来读到时候读不到, 自然就去数据读取最新的维度
        //redisClient.del(key);
    }

    private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSink_table())
                .append("(")
                .append(tp.getSink_columns())
                .append(")values(")
                .append(tp.getSink_columns().replaceAll("[^,]+", "?"))
                .append(")");
        System.out.println("插入语句："+sql.toString());
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 使用数据中的每个字段的值, 给占位符赋值 TODO
        // upsert into t(id, name, age) values(?,?,?);
        //取出字段名
        String[] columns = tp.getSink_columns().split(",");
        for (int i = 0; i < columns.length; i++) {
            String key = columns[i];
            String v = data.getString(key);
            ps.setString(i+1 , v == null ? null : v);
        }

        ps.execute();
        conn.commit();
        ps.close();
    }

    private void checktable(Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {
        if (tableCreateState.value() == null) {


        TableProcess tp = value.f1;
        //建表，执行sql ddl 建表语句
        //create table if not exists t(id varchar ,age varchar, name varchar , constraint pk primary key(id ,age) );
        //phoenix必须要有主键，因为hbase要rowkey
        StringBuilder sql = new StringBuilder();
        //拼接sql语句
            sql
                    .append("create table if not exists ")
                    .append(tp.getSink_table())
                    // id ,activity_name,activity_type,activity_desc,start_time,end_time,create_time
                    .append("(")
                    .append(tp.getSink_columns().replaceAll("([^,]+)", "$1 varchar"))
                    .append(", constraint pk primary key(")
                    .append(tp.getSink_pk() == null ? "id" : tp.getSink_pk())
                    .append("))")
                    .append(tp.getSink_extend() == null ? "" : tp.getSink_extend()); // "null"
        System.out.println("建表语句: " + sql.toString());
        PreparedStatement ps = conn.prepareStatement(sql.toString());

        // 一般情况下, sql语句中可能会存在占位符 ? , 需要给占位符进行赋值
        ps.execute();
        conn.commit();
        ps.close();

        tableCreateState.update(true);
    }
}
}