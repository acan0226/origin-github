package net.acan.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.app.BaseAppV1;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import net.acan.gmall.realtime.util.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/*
cep技术找出来跳出明细

1. 明细应该是一个入口页面(无上一级页面)
    last_page_id = null

2. 后面没有新的页面访问
    过了一段时间仍然没有新的页面

 */
public class DwmUserJumpApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwmUserJumpApp().init(3002,
                1,
                "DwmUserJumpApp_1",
                "DwmUserJumpApp_1",
                Constant.TOPIC_DWD_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {

        stream =
                env.fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                       // "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":14000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":50000} "
                );

        //解析, 添加水印  按照mid分组
        KeyedStream<JSONObject, String> keyedStream = stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));

        //1.定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry")
                .where(new SimpleCondition<JSONObject>() {
                    //入口
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastId = value.getJSONObject("page").getString("last_page_id");

                        return lastId == null || lastId.length() == 0;
                    }
                })
                .next("normal")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastId = value.getJSONObject("page").getString("last_page_id");
                        return lastId != null && lastId.length() > 0;
                    }
                })
                .within(Time.seconds(5));

        //2.把模式作用在流上
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);
        //3.从模式流中获取匹配到的数据，或者超时的数据
        SingleOutputStreamOperator<JSONObject> normal = ps.select(new OutputTag<JSONObject>("jump") {},
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map,
                                              long timeoutTimestamp) throws Exception {
                        return map.get("entry").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return null;
                    }
                }
        );
        normal.getSideOutput(new OutputTag<JSONObject>("jump") {}).print();
    }
}
