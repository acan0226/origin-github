package net.acan.gmall.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.bean.TableProcess;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.JdbcUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection conn;
    private ValueState<Boolean> tableCreateState;

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