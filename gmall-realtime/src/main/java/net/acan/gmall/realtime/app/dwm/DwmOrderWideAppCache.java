package net.acan.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.app.BaseAppV2;
import net.acan.gmall.realtime.bean.OrderDetail;
import net.acan.gmall.realtime.bean.OrderInfo;
import net.acan.gmall.realtime.bean.OrderWide;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.DimUtil;
import net.acan.gmall.realtime.util.JdbcUtil;
import net.acan.gmall.realtime.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;

public class DwmOrderWideAppCache extends BaseAppV2 {
    public static void main(String[] args) {
        new DwmOrderWideAppCache().init(3003,
                1,
                "DwmOrderWideApp",
                "DwmOrderWideApp",
                Constant.TOPIC_DWD_ORDER_INFO,
                Constant.TOPIC_DWD_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       HashMap<String, DataStreamSource<String>> topicStreamMap) {
        //topicStreamMap.get(Constant.TOPIC_DWD_ORDER_INFO).print("info");
        //topicStreamMap.get(Constant.TOPIC_DWD_ORDER_DETAIL).print("detail");


        //事实表join,得到一个新的流（事实表）
        SingleOutputStreamOperator<OrderWide> orderWidewithoutDimStream = factsJoin(topicStreamMap);
        //orderWidewithoutDimStream.print();
        //事实表和维度表join
        factsJoinDim(orderWidewithoutDimStream);
    }

    private void factsJoinDim(SingleOutputStreamOperator<OrderWide> orderWidewithoutDimStream) {
          /*
        join维度的思路:
            拿到每个order_wide 根据 相应的维度表的id去phoenix中查找对应的维度信息

            涉及到了6张维度表 user_info base_province sku_info spu_info  base_trademark base_category3

            使用jdbc的方式
         */
        orderWidewithoutDimStream
                .map(new RichMapFunction<OrderWide, OrderWide>() {

                    private Jedis redisClient;
                    private Connection phoenixConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //phoenix連接
                        phoenixConn = JdbcUtil.getJDBCConnection(Constant.PHONEIX_DRIVER, Constant.PHONEIX_URL, null, null);
                        //redis連接
                        redisClient = RedisUtil.getRedisClient();
                    }

                    @Override
                    public void close() throws Exception {
                        if (phoenixConn == null && !phoenixConn.isClosed()) {
                            phoenixConn.close();
                        }
                        if (redisClient != null) {
                            redisClient.close();//关闭客户端
                            // 如果客户端是从连接池获取的, 则是是归还客户端
                            // 如果客户端是通过 new Jedis(), 则是关闭
                        }
                    }

                    @Override
                    public OrderWide map(OrderWide orderWide) throws Exception {
                        // 读取维度数据, 6张表
                        // 1.补齐user_info,从phoenix中读取，
                        //参数1：连接 参数二：表名 参数三：查询的ID
                        JSONObject userInfo = DimUtil.readDim(redisClient,phoenixConn,"dim_user_info",orderWide.getUser_id());
                        orderWide.setUser_gender(userInfo.getString("GENDER"));
                        orderWide.calculateAge(userInfo.getString("BIRTHDAY"));
                        //补齐省份
                        JSONObject baseProvince = DimUtil.readDim(redisClient,phoenixConn, "dim_base_province", orderWide.getProvince_id());
                        orderWide.setProvince_name(baseProvince.getString("NAME"));
                        orderWide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                        orderWide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));
                        //3.sku_info
                        JSONObject skuInfo = DimUtil.readDim(redisClient,phoenixConn, "dim_sku_info", orderWide.getSku_id());
                        orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                        orderWide.setSku_price(skuInfo.getBigDecimal("PRICE"));
                        orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                        orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                        // 4. spu_info
                        JSONObject spuInfo = DimUtil.readDim(redisClient,phoenixConn, "dim_spu_info", orderWide.getSpu_id());
                        orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
                        //5.base_trademark
                        JSONObject baseTrademark = DimUtil.readDim(redisClient,phoenixConn, "dim_base_trademark", orderWide.getTm_id());
                        orderWide.setTm_name(baseTrademark.getString("TM_NAME"));
                        //6.c3
                        JSONObject c3 = DimUtil.readDim(redisClient,phoenixConn, "dim_base_category3", orderWide.getCategory3_id());
                        orderWide.setCategory3_name(c3.getString("NAME"));

                        return orderWide;
                    }
                }).print();
    }

    private SingleOutputStreamOperator<OrderWide> factsJoin(HashMap<String, DataStreamSource<String>> topicStreamMap) {
        /*
         双流join, 有两种:
          1. 窗口join
          2. interval join
            a: keyBy之后
            b: 只支持事件时间
         */
        KeyedStream<OrderInfo, Long> orderInfoStream = topicStreamMap
                .get(Constant.TOPIC_DWD_ORDER_INFO)
                .map(info -> JSON.parseObject(info, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((info, ts) -> info.getCreate_ts())
                )
                .keyBy(OrderInfo::getId);//订单Id

        KeyedStream<OrderDetail, Long> orderDetailStream = topicStreamMap
                .get(Constant.TOPIC_DWD_ORDER_DETAIL)
                .map(detail -> JSON.parseObject(detail, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((info, ts) -> info.getCreate_ts())
                )
                .keyBy(OrderDetail::getOrder_id);//订单Id

       return orderInfoStream
                .intervalJoin(orderDetailStream)
                .between(Time.seconds(-10),Time.seconds(10))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left,
                                               OrderDetail right,
                                               Context ctx,
                                               Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left,right));
                    }
                });

    }
}
