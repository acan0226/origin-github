package net.acan.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import net.acan.gmall.realtime.app.BaseAppV2;
import net.acan.gmall.realtime.bean.OrderWide;
import net.acan.gmall.realtime.bean.PaymentInfo;
import net.acan.gmall.realtime.bean.PaymentWide;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;

public class DwnPaymentWideApp extends BaseAppV2 {
    public static void main(String[] args) {
        new DwnPaymentWideApp().init(3004,
                1,
                "DwnPaymentWideApp",
                "DwnPaymentWideApp",
                Constant.TOPIC_DWM_ORDER_WIDE,Constant.TOPIC_DWD_PAYMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       HashMap<String, DataStreamSource<String>> topicStreamMap) {

        KeyedStream<PaymentInfo, Long> paymentInfoStream = topicStreamMap
                .get(Constant.TOPIC_DWD_PAYMENT_INFO)
                .map(json -> JSON.parseObject(json, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((json, ts) -> json.getTs())
                )
                .keyBy(PaymentInfo::getOrder_id);


        KeyedStream<OrderWide, Long> orderWideStream = topicStreamMap
                .get(Constant.TOPIC_DWM_ORDER_WIDE)
                .map(json -> JSON.parseObject(json, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((json, ts) -> json.getTs())
                )
                .keyBy(OrderWide::getOrder_id);

        paymentInfoStream
                .intervalJoin(orderWideStream)
                .between(Time.minutes(-45),Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left,right));
                    }
                })
                .map(pw -> JSON.toJSONString(pw))
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_PAYMENT_WIDE));
    }
}
