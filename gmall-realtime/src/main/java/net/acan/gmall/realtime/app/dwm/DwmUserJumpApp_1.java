package net.acan.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.app.BaseAppV1;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
cep技术找出来跳出明细

1. 明细应该是一个入口页面(无上一级页面)
    last_page_id = null

2. 后面没有新的页面访问
    过了一段时间仍然没有新的页面

 */
public class DwmUserJumpApp_1 extends BaseAppV1 {
    public static void main(String[] args) {
        new DwmUserJumpApp_1().init(3002,
                1,
                "DwmUserJumpApp_1",
                "DwmUserJumpApp_1",
                Constant.TOPIC_DWD_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {

//        stream =
//                env.fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":16000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":50000} "
//                );

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
                .<JSONObject>begin("entry1")
                .where(new SimpleCondition<JSONObject>() {
                    //入口
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastId = value.getJSONObject("page").getString("last_page_id");
                        return lastId == null || lastId.length() == 0;
                    }
                })
                .next("entry2")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastId = value.getJSONObject("page").getString("last_page_id");
                        return lastId == null || lastId.length() == 0;
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
                       //如果第二个没来
                        return map.get("entry1").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        //如果两个都是入口，那第一个就是跳出
                        return map.get("entry1").get(0);
                    }
                }
        );
        normal.getSideOutput(new OutputTag<JSONObject>("jump") {})
                .union(normal)
                //将JSONObject转为字符串String
                .map(obj -> obj.toJSONString())
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.DWM_UJ));

    }
}
