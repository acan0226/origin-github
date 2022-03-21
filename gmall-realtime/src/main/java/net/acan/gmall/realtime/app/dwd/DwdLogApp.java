package net.acan.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.app.BaseAppV1;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import net.acan.gmall.realtime.util.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.*;

public class DwdLogApp extends BaseAppV1 {
    private final String PAGE = "page";
    private final String START = "start";
    private final String DISPLAY = "display";

    public static void main(String[] args) {
        new DwdLogApp().init(20001,
                1,
                "DwdLogApp",
                "DwdLogApp",
                Constant.TOPIC_ODS_LOG);




    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        //具体的业务逻辑

        // 1. 区别新老客户. 把is_new做一个验证
        SingleOutputStreamOperator<JSONObject> validateStream = userNewOrOld(stream);

        // 2. 对数据分流: 页面日志  启动日志 曝光日志
        HashMap<String, DataStream<JSONObject>> threeStreams = splitStream(validateStream);
        // 3. 不同的流写入到不同的topic中
        writeToKafka(threeStreams);
    }

    private void writeToKafka(HashMap<String, DataStream<JSONObject>> threeStreams) {
        threeStreams
                .get(START)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_START));

        threeStreams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_PAGE));

        threeStreams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_DISPLAY));

    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validateStream) {

        OutputTag<JSONObject> pageTag= new OutputTag<JSONObject>("page"){};
        OutputTag<JSONObject> displayTag= new OutputTag<JSONObject>("display"){};
           /*
         启动日志放主流
         页面和曝光分表放入到一个测输出流
         */

        SingleOutputStreamOperator<JSONObject> startStream = validateStream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value,
                                       Context ctx,
                                       Collector<JSONObject> out) throws Exception {
                //启动日志
                if (value.containsKey("start")) {
                    out.collect(value);
                } else {
                    //判断是否为页面
                    if (value.containsKey("page")) {
                        ctx.output(pageTag, value);
                    }
                    // 因为曝光日志中的曝光数据是数组, 最好展开, 分别放入流中
                    if (value.containsKey("displays")) {
                        JSONArray array = value.getJSONArray("displays");
                        for (int i = 0; i < array.size(); i++) {
                            JSONObject display = array.getJSONObject(i);
                            // 给曝光数据增添一些其他字段
                            display.putAll(value.getJSONObject("common"));
                            display.putAll(value.getJSONObject("page"));
                            display.put("ts", value.getLong("ts"));


                            ctx.output(displayTag, display);
                        }

                    }
                }
            }
        });
        DataStream<JSONObject> pageStream = startStream.getSideOutput(pageTag);
        DataStream<JSONObject> displayStream = startStream.getSideOutput(displayTag);

        //返回多个流：1返回一个集合 2.返回元组(a,b,c) 3.map
        HashMap<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put(START, startStream);

        streams.put(PAGE, pageStream);
        streams.put(DISPLAY, displayStream);

        return streams;



    }

    private SingleOutputStreamOperator<JSONObject> userNewOrOld(DataStreamSource<String> stream) {
         /*
         对 is_new做修改: 如果是新用户则设置为1 否则设置为0

         如何判断新老用户?

         这个用户的第一条记录 应该是1 其他的记录应该 0

         使用状态存储时间戳: 第一条数据来的时候, 状态是空, 然后给状态赋值, 后面的数据的再来就不为空

         如果数据有乱序?
            事件时间+水印+窗口

           使用5s的滚动窗口

           找到这个用户的第一个窗口, 时间戳最小的那个才是第一条记录, 其他的都不是

           其他窗口里面所有操作肯定都是0

         */
      return stream
                .map(JSON::parseObject)
               .assignTimestampsAndWatermarks(
                       WatermarkStrategy
                               .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                               .withTimestampAssigner((obj,ts) -> obj.getLong("ts"))
               )
               .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
               .window(TumblingEventTimeWindows.of(Time.seconds(5)))
               .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                   private ValueState<Long> firstWindowState;

                   @Override
                   public void open(Configuration parameters) throws Exception {
                       firstWindowState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstWindowState", Long.class
                       ));
                   }

                   @Override
                   public void process(String s,
                                      Context context,
                                       Iterable<JSONObject> elements,
                                       Collector<JSONObject> out) throws Exception {
                    //判断是否第一个窗口
                       if(firstWindowState.value() == null){
                          //第一个窗口
                           // 找到所有元素中时间戳最小的那个元素, 把他的is_new置为1, 其他的是0
                           List<JSONObject> list = MyUtil.toList(elements);
                           JSONObject min = Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")));
                           firstWindowState.update(min.getLong("ts"));
                           for (JSONObject object : list) {
                               if (object == min) {
                                   object.getJSONObject("common").put("is_new", 1);
                               }else{
                                   object.getJSONObject("common").put("is_new", 0);
                               }
                               out.collect(object);
                           }
                       }else{
                          //不是第一个窗口
                           for (JSONObject element : elements) {
                               element.getJSONObject("common").put("is_new",0);
                               out.collect(element);
                           }
                       }

                   }
               });


    }
}
