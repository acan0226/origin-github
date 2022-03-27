package net.acan.gmall.realtime.app.dws;

import net.acan.gmall.realtime.app.BaseSqlApp;
import net.acan.gmall.realtime.bean.ProvinceStats;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsApp_TVF extends BaseSqlApp {
    public static void main(String[] args) {
        new ProvinceStatsApp_TVF().init(4003,1,"ProvinceStatsApp");
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 1. 建立动态表与Kafka的topic进行关联
        tableEnv.executeSql("create table order_wide(" +
                " province_id bigint, " +
                " province_name string, " +
                " province_area_code string, " +
                " province_iso_code string, " +
                " province_3166_2_code string, " +
                " split_total_amount decimal(20, 2), " +
                " order_id bigint, " +
                " create_time string, " +
                "et as TO_TIMESTAMP(create_time), " +
                "watermark for et as et - interval '3' second " +
                ")with(" +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092', " +
                "   'properties.group.id' = 'ProvinceStatsApp', " +
                "   'topic' = '" + Constant.TOPIC_DWM_ORDER_WIDE + "', " +
                "   'scan.startup.mode' = 'earliest-offset', " +
                "   'format' = 'json' " +
                ")");

        //tableEnv.sqlQuery("select * from order_wide").execute().print();
        //2.开窗聚合
        Table table = tableEnv.sqlQuery("select " +
                "province_id," +
                " province_name," +
                " province_area_code area_code," +
                " province_iso_code iso_code, " +
                " province_3166_2_code iso_3166_2, " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " sum(split_total_amount) order_amount, " +
                " count(distinct(order_id)) order_count," +
                " unix_timestamp()*1000 ts " +//系统时间
                "from table(tumble(table order_wide, descriptor(et), interval '5' second )) " +
                "group by province_id, province_name, province_area_code, province_iso_code, province_3166_2_code, " +
                "window_start, window_end");
        table.execute().print();

        // 3. 把表转成流写出去
//        tableEnv.toRetractStream(table, ProvinceStats.class)
//                .filter(t -> t.f0)
//                .map(t -> t.f1)
//                .addSink(FlinkSinkUtil.getClickHouseSink(
//                        Constant.CLICKHOUSE_DB,
//                        Constant.CLICKHOUSE_TABLE_PROVINCE_STATS_2022,
//                        ProvinceStats.class
//                ));
    }
}
/*

    `stt` DateTime,
    `edt` DateTime,
    `province_id` UInt64,
    `province_name` String,
    `area_code` String,
    `iso_code` String,
    `iso_3166_2` String,
    `order_amount` Decimal64(2),
    `order_count` UInt64,
    `ts` UInt64


地区主题宽表
 每个地区的销售 订单数  ...

1. 确定数据源
    dwm_order_wide

2. 采用的技术
    使用 flink sql

3. 思路
    1. 建立动态表与kafka的topic关联

    2. 读取数据

    3. 开窗聚合
        分组窗口
        over窗口
        tvf
    4. 最后结果写入到clickhouse中
        应该使用纯sql : DDL

        把表转成流, 使用自定sink写入



 */
