package net.acan.gmall.realtime.app.dws;

import net.acan.gmall.realtime.Function.IkAnalyzer;
import net.acan.gmall.realtime.app.BaseSqlApp;
import net.acan.gmall.realtime.bean.KeywordStats;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SearchKeyWordStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        new SearchKeyWordStatsApp().init(4004,1,"SearchKeyWordStatsApp");
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 1. 建立动态表与Kafka的topic进行关联
        tableEnv.executeSql("create table page(" +
                "page map<string,string>, " +
                "ts bigint, " +
                "et as to_timestamp_ltz(ts,3), " +//时间戳转为日期时间
                "watermark for et as et - interval '3' second " +
                ")with(" +
                "'connector' = 'kafka', " +
                " 'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092', " +
                " 'properties.group.id' = 'SearchKeyWordStatsApp', " +
                " 'topic' = '"+ Constant.TOPIC_DWD_PAGE +"', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json' " +
                ")");

        //2. 过滤出来搜索计算, 把搜索关键词取出
        Table t1 = tableEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from page " +
                "where page['page_id']='good_list' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");

        tableEnv.createTemporaryView("t1", t1);

        // 3. 对关键词进行分词
        // fink 中没有提供直接可用的函数, 所有需要自定义函数
        // scalar function  标量函数 一进一出
        // table function  制表函数，UDTF,一进多出
        // aggregate function 聚合函数，UDAF,多进一出
        // tableaggregate function
        // 分词选择制表函数

        //3.1注册函数
        tableEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);

        Table t2 = tableEnv.sqlQuery("select " +
                "word, " +
                "et " +
                "from t1 " +
                "join lateral table( ik_analyzer(kw)) on true");

        tableEnv.createTemporaryView("t2",t2);

        //4.开窗聚合
        Table resultTable = tableEnv.sqlQuery("select " +
                "CONVERT_TZ(date_format(window_start,'yyyy-MM-dd HH:mm:ss'),'UTC','Asia/Shanghai') stt, " +//CONVERT_TZ()设置时间的时区
                "CONVERT_TZ(date_format(window_end,'yyyy-MM-dd HH:mm:ss'),'UTC','Asia/Shanghai') edt, " +//date_format()将时间戳转为指定格式的时间
                " word keyword, " +
                " 'search' source, " +
                " count(*) ct, " +
                " unix_timestamp() * 1000 ts " +//当前时间的时间戳
                "from table(tumble(table t2, descriptor(et), interval '5' second) )" +//使用TVF函数 table value function
                "group by word,window_start,window_end");


        //5.将sql转为流，写入到clickhouse，
        tableEnv.toRetractStream(resultTable, KeywordStats.class)
                .filter(a -> a.f0)
                .map(a -> a.f1)
                .addSink(FlinkSinkUtil.getClickHouseSink(
                        Constant.CLICKHOUSE_DB,
                        Constant.CLICKHOUSE_TABLE_KEYWORD_STATS_2022,
                        KeywordStats.class));
    }
}
