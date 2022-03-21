package net.acan.gmall.realtime.common;

public class Constant {
    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";
    public static final String SINK_KAFKA =  "kafka";
    public static final String SINK_HBASE =  "hbase";

    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_DWD_START = "dwd_start";
    public static final String TOPIC_DWD_PAGE = "dwd_page";
    public static final String TOPIC_DWD_DISPLAY = "dwd_display";
    public static final String PHONEIX_DRIVER ="org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHONEIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    public static final String TOPIC_DWM_UV = "dwm_uv";
    public static final String DWM_UJ = "dwm_uj";
    public static final String TOPIC_DWD_ORDER_INFO = "dwd_order_info";
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd_order_detail";
}
