package net.acan.gmall.realtime.app.dws;

import net.acan.gmall.realtime.Function.IkAnalyzer;
import net.acan.gmall.realtime.Function.KwProduct;
import net.acan.gmall.realtime.app.BaseSqlApp;
import net.acan.gmall.realtime.bean.KeywordStats;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProductKeyWordStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        new ProductKeyWordStatsApp().init(40054,1,"ProductKeyWordStatsApp");
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 1. 建立动态表与Kafka的topic进行关联
        tableEnv.executeSql("create table product(" +
                "stt string, " +
                "edt string, " +
                "sku_name string, " +//时间戳转为日期时间
                "click_ct bigint, " +
                "cart_ct bigint, " +
                "order_ct bigint " +
                ")with(" +
                "'connector' = 'kafka', " +
                " 'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092', " +
                " 'properties.group.id' = 'SearchKeyWordStatsApp', " +
                " 'topic' = '"+ Constant.TOPIC_DWS_PRODUCT_STATS +"', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json' " +
                ")");

        //tableEnv.sqlQuery("select * from product").execute().print();

        //1.过滤出来三个ct中至少有一个不为0的记录
        Table t1 = tableEnv.sqlQuery("select " +
                "* " +
                "from product " +
                "where click_ct > 0 " +
                "or cart_ct > 0 " +
                "or order_ct > 0");


        tableEnv.createTemporaryView("t1", t1);

        //对sku_name进行分词
        // 2.1 注册分词函数
        tableEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);

        Table t2 = tableEnv.sqlQuery("select " +
                "stt, edt, word, click_ct, cart_ct, order_ct " +
                "from t1, " +
                "lateral table(ik_analyzer(sku_name))");
        tableEnv.createTemporaryView("t2", t2);

        // 3. 把一行中的三个count 列, 变成3行, 自定义 table函数     click  10
        //需要进行  行转列  一行转为三列
        tableEnv.createTemporaryFunction("kw_product", KwProduct.class);

        Table t3 = tableEnv.sqlQuery("select " +
                "stt, " +
                "edt, " +
                "word keyword, " +
                "source, " +
                "ct " +
                "from t2 " +
                "join lateral table( kw_product(click_ct, cart_ct, order_ct)) on true");

        tableEnv.createTemporaryView("t3", t3);

        Table resultTable = tableEnv.sqlQuery("select " +
                " stt, " +
                " edt, " +
                " keyword, " +
                " source, " +
                " sum(ct) ct," +
                " unix_timestamp() * 1000 ts " +
                "from t3 " +
                "group by stt,edt,keyword,source");


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
