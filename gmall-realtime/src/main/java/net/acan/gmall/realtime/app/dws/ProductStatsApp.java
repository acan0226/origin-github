package net.acan.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.Function.DimAsyncFunction;
import net.acan.gmall.realtime.app.BaseAppV2;
import net.acan.gmall.realtime.bean.OrderWide;
import net.acan.gmall.realtime.bean.PaymentWide;
import net.acan.gmall.realtime.bean.ProductStats;
import net.acan.gmall.realtime.util.DimUtil;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import net.acan.gmall.realtime.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static net.acan.gmall.realtime.common.Constant.*;

public class ProductStatsApp extends BaseAppV2 {
    public static void main(String[] args) {
        new ProductStatsApp().init(4002,
                1,"ProductStatsApp","ProductStatsApp",
                TOPIC_DWD_PAGE, TOPIC_DWD_DISPLAY,
                TOPIC_DWD_FAVOR_INFO, TOPIC_DWD_CART_INFO,
                TOPIC_DWM_ORDER_WIDE, TOPIC_DWM_PAYMENT_WIDE,
                TOPIC_DWD_REFUND_PAYMENT, TOPIC_DWD_COMMENT_INFO
                );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> topicStreamMap) {
        //1、union成一个流
        DataStream<ProductStats> psStream = unionOne(topicStreamMap);
        //psStream.print();
        //2、开窗聚合
        SingleOutputStreamOperator<ProductStats> psStreamWithoutDim = windowAndAgg(psStream);
        //psStreamWithoutDim.print();
        // 3. join维度信息  产品相关的维度信息: sku spu tm c3
        SingleOutputStreamOperator<ProductStats> psSreamWithDim = joinDim(psStreamWithoutDim);
        //psSreamWithDim.print();
        // 4. 写入到Clickhouse中
        writeToClickHouse(psSreamWithDim);


    }

    private void writeToClickHouse(SingleOutputStreamOperator<ProductStats> stream) {
        stream.addSink(FlinkSinkUtil.getClickHouseSink(CLICKHOUSE_DB,
                CLICKHOUSE_TABLE_PRODUCT_STATS_2022,
                ProductStats.class));
    }

    private SingleOutputStreamOperator<ProductStats> joinDim(SingleOutputStreamOperator<ProductStats> stream) {
       return AsyncDataStream
                .unorderedWait(stream,
                        new DimAsyncFunction<ProductStats>() {
                            @Override
                            public void addDim(Jedis redisClient,
                                               Connection phoenixConn,
                                               ProductStats input,
                                               ResultFuture<ProductStats> resultFuture) throws Exception {
                                // 3. sku_info
                                JSONObject skuInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_sku_info", input.getSku_id());
                                input.setSku_name(skuInfo.getString("SKU_NAME"));
                                input.setSku_price(skuInfo.getBigDecimal("PRICE"));

                                input.setSpu_id(skuInfo.getLong("SPU_ID"));
                                input.setTm_id(skuInfo.getLong("TM_ID"));
                                input.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                                // 4. spu_info
                                JSONObject spuInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_spu_info", input.getSpu_id());
                                input.setSpu_name(spuInfo.getString("SPU_NAME"));
                                // 5. base_trademark
                                JSONObject baseTrademark = DimUtil.readDim(redisClient, phoenixConn, "dim_base_trademark", input.getTm_id());
                                input.setTm_name(baseTrademark.getString("TM_NAME"));


                                // 6. c3
                                JSONObject c3 = DimUtil.readDim(redisClient, phoenixConn, "dim_base_category3", input.getCategory3_id());
                                input.setCategory3_name(c3.getString("NAME"));

                                resultFuture.complete(Collections.singletonList(input));
                            }
                        },
                        60,
                        TimeUnit.SECONDS);
    }

    private SingleOutputStreamOperator<ProductStats> windowAndAgg(DataStream<ProductStats> psStream) {
        return psStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                )
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats ps1,
                                                       ProductStats ps2) throws Exception {
                                ps1.setClick_ct(ps1.getClick_ct() + ps2.getClick_ct());
                                ps1.setDisplay_ct(ps1.getDisplay_ct() + ps2.getDisplay_ct());

                                ps1.setFavor_ct(ps1.getFavor_ct() + ps2.getFavor_ct());
                                ps1.setCart_ct(ps1.getCart_ct() + ps2.getCart_ct());

                                ps1.setOrder_sku_num(ps1.getOrder_sku_num() + ps2.getOrder_sku_num());
                                ps1.setOrder_amount(ps1.getOrder_amount().add(ps2.getOrder_amount()));
                                ps1.getOrderIdSet().addAll(ps2.getOrderIdSet());

                                ps1.setPayment_amount(ps1.getPayment_amount().add(ps2.getPayment_amount()));
                                ps1.getPaidOrderIdSet().addAll(ps2.getPaidOrderIdSet());

                                ps1.setRefund_amount(ps1.getRefund_amount().add(ps2.getRefund_amount()));
                                ps1.getRefundOrderIdSet().addAll(ps2.getRefundOrderIdSet());


                                ps1.setComment_ct(ps1.getComment_ct() + ps2.getComment_ct());
                                ps1.setGood_comment_ct(ps1.getGood_comment_ct() + ps2.getGood_comment_ct());


                                return ps1;
                            }
                        }, new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void process(Long skuId,
                                                Context ctx,
                                                Iterable<ProductStats> elements,
                                                Collector<ProductStats> out) throws Exception {
                                ProductStats ps = elements.iterator().next();

                                ps.setStt(MyUtil.toDateTime(ctx.window().getStart()));
                                ps.setEdt(MyUtil.toDateTime(ctx.window().getEnd()));

                                ps.setTs(System.currentTimeMillis());

                                ps.setOrder_ct((long) ps.getOrderIdSet().size());
                                ps.setPaid_order_ct((long) ps.getPaidOrderIdSet().size());
                                ps.setRefund_order_ct((long) ps.getRefundOrderIdSet().size());


                                out.collect(ps);
                            }
                        }
                );


    }

    private DataStream<ProductStats> unionOne(HashMap<String, DataStreamSource<String>> topicStreamMap) {
            /*
            点击	多维分析
            曝光	多维分析
            收藏	多维分析
            加入购物车	多维分析
            下单	可视化大屏
            支付	多维分析
            退款	多维分析
            评价	多维分析
     */

        // 1. 点击
        SingleOutputStreamOperator<ProductStats> clickStream = topicStreamMap
                .get(TOPIC_DWD_PAGE)
                .flatMap(new FlatMapFunction<String, ProductStats>() {
                    @Override
                    public void flatMap(String value,
                                        Collector<ProductStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(value);

                        // 判断是否为点击
                        JSONObject page = obj.getJSONObject("page");
                        String itemType = page.getString("item_type");
                        if ("sku_id".equals(itemType)) {  //  表示点击了一个商品
                            ProductStats ps = new ProductStats();

                            Long ts = obj.getLong("ts");
                            Long sku_id = page.getLong("item");

                            ps.setSku_id(sku_id);
                            ps.setClick_ct(1L);

                            ps.setTs(ts);

                            out.collect(ps);
                        }

                    }
                });

        // 2. 曝光
        SingleOutputStreamOperator<ProductStats> displayStream = topicStreamMap
                .get(TOPIC_DWD_DISPLAY)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value,
                                               Context ctx,
                                               Collector<ProductStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        // 判断是否为点击
                        String itemType = obj.getString("item_type");
                        if ("sku_id".equals(itemType)) {  //  表示点击了一个商品
                            ProductStats ps = new ProductStats();

                            Long ts = obj.getLong("ts");
                            Long sku_id = obj.getLong("item");

                            ps.setSku_id(sku_id);
                            ps.setDisplay_ct(1L);

                            ps.setTs(ts);

                            out.collect(ps);
                        }
                    }
                });
        // 3. 收藏
        SingleOutputStreamOperator<ProductStats> favorInfoStream = topicStreamMap
                .get(TOPIC_DWD_FAVOR_INFO)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    ProductStats ps = new ProductStats();

                    Long sku_id = obj.getLong("sku_id");
                    Long ts = MyUtil.toTs(obj.getString("create_time"));

                    ps.setSku_id(sku_id);
                    ps.setFavor_ct(1L);

                    ps.setTs(ts);

                    return ps;
                });
        // 4. 加购物车
        SingleOutputStreamOperator<ProductStats> cartInfoStream = topicStreamMap
                .get(TOPIC_DWD_CART_INFO)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    ProductStats ps = new ProductStats();

                    Long sku_id = obj.getLong("sku_id");
                    Long ts = MyUtil.toTs(obj.getString("create_time"));

                    ps.setSku_id(sku_id);
                    ps.setCart_ct(1L);

                    ps.setTs(ts);

                    return ps;
                });
        // 5. 订单
        SingleOutputStreamOperator<ProductStats> orderWideStream = topicStreamMap
                .get(TOPIC_DWM_ORDER_WIDE)
                .map(json -> {

                    OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
                    ProductStats ps = new ProductStats();

                    ps.setSku_id(orderWide.getSku_id());
                    ps.setOrder_amount(orderWide.getSplit_total_amount());  // 获取这个商品分摊的总金额
                    ps.setOrder_sku_num(orderWide.getSku_num());
                    ps.getOrderIdSet().add(orderWide.getOrder_id());  // 将来用(聚合完)来计算订单数

                    ps.setTs(orderWide.getTs());

                    return ps;

                });
        // 6. 支付
        SingleOutputStreamOperator<ProductStats> paymentWideStream = topicStreamMap
                .get(TOPIC_DWM_PAYMENT_WIDE)
                .map(json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    ProductStats ps = new ProductStats();

                    ps.setSku_id(paymentWide.getSku_id());
                    ps.setPayment_amount(paymentWide.getSplit_total_amount());

                    ps.getPaidOrderIdSet().add(paymentWide.getOrder_id());

                    ps.setTs(MyUtil.toTs(paymentWide.getPayment_create_time()));
                    return ps;

                });
        // 7. 退款
        SingleOutputStreamOperator<ProductStats> refundStream = topicStreamMap
                .get(TOPIC_DWD_REFUND_PAYMENT)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(obj.getLong("sku_id"));
                    ps.setRefund_amount(obj.getBigDecimal("total_amount"));
                    ps.getRefundOrderIdSet().add(obj.getLong("order_id"));


                    ps.setTs(MyUtil.toTs(obj.getString("create_time")));
                    return ps;

                });
        // 8. 评价
        SingleOutputStreamOperator<ProductStats> commentInfoStream = topicStreamMap
                .get(TOPIC_DWD_COMMENT_INFO)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(obj.getLong("sku_id"));
                    ps.setComment_ct(1L);

                    String appraise = obj.getString("appraise");
                    if (FIVE_STAR_GOOD_COMMENT.equals(appraise) || FOUR_STAR_GOOD_COMMENT.equals(appraise)) {
                        ps.setGood_comment_ct(1L);
                    }


                    ps.setTs(MyUtil.toTs(obj.getString("create_time")));
                    return ps;
                });

        return clickStream.union(displayStream,
                favorInfoStream,
                cartInfoStream,
                orderWideStream,
                paymentWideStream,
                refundStream,
                commentInfoStream

        );
    }
}
