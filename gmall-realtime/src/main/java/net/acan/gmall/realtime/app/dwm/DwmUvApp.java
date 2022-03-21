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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/*
为了dws层计算uv值, 做提前处理: 去重

dau 日活
1. 数据源? 选择 dwd_page
    kafka的dwd层的日志相关的数据

    dwd_start

    dwd_page

    dwd_display

2. uv实现逻辑?
 去重

  使用flink的状态

  使用窗口 + 事件时间

  考虑跨天的问题

 */
public class DwmUvApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwmUvApp().init(3001,
                1,
                "DwmUvApp",
                "DwmUvApp",
                Constant.TOPIC_DWD_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((obj,ts)->obj.getLong("ts"))
                )
                .keyBy(t -> t.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                    private SimpleDateFormat sdf;
                    private ValueState<String> dateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("dateState", String.class));

                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }


                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {

                        String yesterday = dateState.value();
                        String today = sdf.format(context.window().getStart());

                        if (!today.equals(yesterday)) {
                            //不相等，说明跨天了
                            dateState.clear();
                        }

                        if (dateState.value() == null) {
                        //表示第一个窗口，也只有第一个窗口才要必要处理
                        List<JSONObject> list = MyUtil.toList(elements);
                            JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                            out.collect(min);
                            //更新状态
                            dateState.update(sdf.format(min.getLong("ts")));
                        }

                    }
                })
                .map(obj -> obj.toJSONString())
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_UV));
    }
}
