package net.acan.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.app.BaseAppV1;
import net.acan.gmall.realtime.bean.TableProcess;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class DwdDbApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwdDbApp().init(2002,
                1,
                "DwdDbApp",
                "DwdDbApp",
                Constant.TOPIC_ODS_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
       // stream.print();

        // 1. 对业务数据做etl
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        // 2. 读取配置表的数据
       SingleOutputStreamOperator<TableProcess> tpStream =
       readTableProcess(env);
        // 3. 数据流和配置流进行connect, 实现动态分流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream = connectStream(etlStream, tpStream);
        //connectStream.print();
        //4.过滤掉不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColums = filterColumn(connectStream);
        //5、动态分流
        Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> kafkaHbaseStream =
                dynamicSplitStream(filterColums);
        //kafkaHbaseStream.f0.print("kafka");
        //kafkaHbaseStream.f1.print("hbase");
        // 6. 不同的数据写入到不同的sink
        writeToKafka(kafkaHbaseStream.f0);
        writeToHbase(kafkaHbaseStream.f1);
    }

    private void writeToHbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {
        //自定义sink 使用jdbc来建表和插入数据
        stream
                .keyBy(t -> t.f1.getSink_table())
                .addSink(FlinkSinkUtil.getPhoenixSink());
    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        //将数据写入到kafka中,只需要写JSONObject数据，不用配置的表的数据
        stream
                .addSink(FlinkSinkUtil.getKafkaSink());


    }

    private Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> dynamicSplitStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag =
                new OutputTag<Tuple2<JSONObject, TableProcess>>("habse") {};
            /*
        数据一共有两个sink:  kafka hbase(phoenix)

        分两个流: 一个流(事实表)到kafka 一个流(维度表)到phoenix  执行sql , 写明表名

        主流  kafka
        侧输出流 phoenix

         */
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = stream
                .process(new ProcessFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(Tuple2<JSONObject, TableProcess> value,
                                               Context ctx, Collector<Tuple2<JSONObject,
                            TableProcess>> out) throws Exception {
                        String sinkType = value.f1.getSink_type();
                        if (Constant.SINK_KAFKA.equals(sinkType)) {
                            out.collect(value);
                        } else if (Constant.SINK_HBASE.equals(sinkType)) {
                            ctx.output(hbaseTag, value);
                        }
                    }
                });
        DataStream<Tuple2<JSONObject, TableProcess>> habseStream = kafkaStream.getSideOutput(hbaseTag);
        return Tuple2.of(kafkaStream,habseStream);
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumn(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream) {
        return connectStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {

                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                        //过滤不需要的字段
                        JSONObject data = value.f0;
                        //id,activity_name,activity_type,activity_desc,start_time,end_time,create_tim
                        List<String> columns = Arrays.asList(value.f1.getSink_columns().split(","));
                        //遍历data中的每一个列名，如果存在columns这个集合中，就保留，不在就删除
                        //如何删除？参考mapDemo
                        Set<String> keys = data.keySet();
                        Iterator<String> it = keys.iterator();
                        while (it.hasNext()) {
                            String key = it.next();
                            if (!columns.contains(key)) {
                                it.remove();
                            }
                        }

                        return value;
                    }
                });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream(SingleOutputStreamOperator<JSONObject> dataStream,
                                                                                       SingleOutputStreamOperator<TableProcess> tpStream) {
    //1.配置流做成广播流
        //广播的底层本质就是map
        // key字符串  （配置表realtime的表）  表名+操作类型   order_info:insert
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);

        BroadcastStream<TableProcess> bcStream =
                tpStream.broadcast(tpStateDesc);
        //2、数据流去connect广播流
           return dataStream.connect(bcStream)
                    .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                        // 根据表名:操作类型, 从广播状态中国获取对应的配置信息
                        @Override
                        public void processElement(JSONObject value,
                                                   ReadOnlyContext ctx,
                                                   Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                            ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                            // 拼接处key
                    /*
                    {
                      "database": "gmall2022",
                      "table": "order_status_log",
                      "type": "insert",
                      "ts": 1647411239,
                      "xid": 6112,
                      "xoffset": 3995,
                      "data": {
                        "id": 69122,
                        "order_id": 26647,
                        "order_status": "1001",
                        "operate_time": "2022-03-16 14:13:59"
                      }
                    }
                     */
                            String key = value.getString("table") + "_" + value.getString("type");
                            TableProcess tp = tpState.get(key);
                            // 有些表在配置信息中并没有, 表示这张表的数据不需要sink
                            // 如果这张表的配置不存在, tp对象就是null
                        if (tp != null) {

                            out.collect(Tuple2.of(value.getJSONObject("data"),tp));
                            }
                        }
                        // 处理广播流中的数据
                        @Override
                        public void processBroadcastElement(TableProcess value,
                                                           Context ctx,
                                                            Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //把流中的每个配置，写入到广播状态中
                            BroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                            String key = value.getSource_table() + "_" + value.getOperate_type();
                            tpState.put(key,value);
                        }
                    });
    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        //创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE `table_process` (" +
                "  `source_table` string," +
                "  `operate_type` string," +
                "  `sink_type` string," +
                "  `sink_table` string," +
                "  `sink_columns` string," +
                "  `sink_pk` string," +
                "  `sink_extend` string," +
                "  PRIMARY KEY (`source_table`,`operate_type`)not enforced" +
                ")with("+
                " 'connector' = 'mysql-cdc'," +
                        " 'hostname' = 'hadoop162'," +
                        " 'port' = '3306'," +
                        " 'username' = 'root'," +
                        " 'password' = 'aaaaaa'," +
                        " 'database-name' = 'gmall2022_realtime'," +
                        " 'table-name' = 'table_process', " +
                        // 程序第一次启动先读取表中所有数据, 然后在使用binlog实时监控变化
                        " 'debezium.snapshot.mode'='initial'" +
                ")"
        );
        //Table tp = tableEnv.sqlQuery("select * from table_process");

        Table tp = tableEnv.from("table_process");
        //把这张表转为一个流
        return tableEnv.toRetractStream(tp, TableProcess.class)
                .filter(s -> s.f0)
                .map(t -> t.f1);
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
                /*
            {
              "database": "gmall2022",
              "table": "order_status_log",
              "type": "insert",
              "ts": 1647410652,
              "xid": 297,
              "xoffset": 4040,
              "data": {
                "id": 69053,
                "order_id": 26623,
                "order_status": "1001",
                "operate_time": "2022-03-16 14:04:12"
              }
            }
         */
         return stream
                .map(data -> JSON.parseObject(data.replaceAll("bootstrap-", "")))
                .filter(obj ->
                        "gmall2022".equals(obj.getString("database"))
                && obj.getString("table") != null
                && ("insert".equals(obj.getString("type")) || "update".equals(obj.getString("type")))
                && obj.getString("data") != null
                && obj.getString("data") .length() >2
                );
    }
}
