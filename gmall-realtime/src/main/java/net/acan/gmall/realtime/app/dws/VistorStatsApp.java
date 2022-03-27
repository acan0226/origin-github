package net.acan.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.app.BaseAppV2;

import net.acan.gmall.realtime.bean.VisitorStats;
import net.acan.gmall.realtime.common.Constant;
import net.acan.gmall.realtime.util.FlinkSinkUtil;
import net.acan.gmall.realtime.util.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;

import static net.acan.gmall.realtime.common.Constant.*;


public class VistorStatsApp extends BaseAppV2 {
    public static void main(String[] args) {
        new VistorStatsApp().init(4001,
                1,
                "VisitorStatsApp",
                "VisitorStatsApp",
                TOPIC_DWD_PAGE,TOPIC_DWM_UV,DWM_UJ);
    }
    @Override
    public void handle(StreamExecutionEnvironment env,
                       HashMap<String, DataStreamSource<String>> topicStreamMap) {
        //1.把多个流union成一个流，先保证把流的类型弄统一，用POJO类
        DataStream<VisitorStats> vsStream = unionOne(topicStreamMap);
       // vsStream.print();
        //2.开窗聚合
        SingleOutputStreamOperator<VisitorStats> vsAggStream = windowAndAgg(vsStream);
        //3.数据写入到ClickHouse中
        writeToClickHouse(vsAggStream);
    }

    private void writeToClickHouse(SingleOutputStreamOperator<VisitorStats> vsAggStream) {
               /*
        没有专门clickhouseSink, 在jdbcSink的基础上进行封装, 得到一个ClickhouseSink
         */
        vsAggStream
                .addSink(FlinkSinkUtil.getClickHouseSink(Constant.CLICKHOUSE_DB,
                                Constant.CLICKHOUSE_TABLE_VISITOR_STATS_2022,
                                VisitorStats.class)
                        );
    }

    private SingleOutputStreamOperator<VisitorStats> windowAndAgg(DataStream<VisitorStats> vsStream) {
        //基于事件时间
        SingleOutputStreamOperator<VisitorStats> result = vsStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                )
                .keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<VisitorStats>("late") {})
                // sum reduce aggregate process
                .reduce(new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats vs1,
                                                       VisitorStats vs2) throws Exception {
                                vs1.setPv_ct(vs1.getPv_ct() + vs2.getPv_ct());
                                vs1.setUv_ct(vs1.getUv_ct() + vs2.getUv_ct());
                                vs1.setUj_ct(vs1.getUj_ct() + vs2.getUj_ct());
                                vs1.setSv_ct(vs1.getSv_ct() + vs2.getSv_ct());
                                vs1.setDur_sum(vs1.getDur_sum() + vs2.getDur_sum());
                                return vs1;
                            }
                        },
                        new ProcessWindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                Context ctx,
                                                Iterable<VisitorStats> elements,// reduce和agg后只有一个值, 就是前面聚合的最终结果
                                                Collector<VisitorStats> out) throws Exception {
                                VisitorStats vs = elements.iterator().next();
                                String stt = MyUtil.toDateTime(ctx.window().getStart());
                                String edt = MyUtil.toDateTime(ctx.window().getEnd());

                                vs.setStt(stt);
                                vs.setEdt(edt);

                                vs.setTs(System.currentTimeMillis());  // 改成统计时间

                                out.collect(vs);

                            }
                        }

                );
        return result;

    }

    private DataStream<VisitorStats> unionOne(HashMap<String, DataStreamSource<String>> topicStreamMap) {

        //pv和持续时间
        SingleOutputStreamOperator<VisitorStats> pvAndDtStream = topicStreamMap.get(TOPIC_DWD_PAGE)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    Long duringTime = obj.getJSONObject("page").getLong("during_time");
                    Long ts = obj.getLong("ts");
                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, is_new,
                            0L, 1L, 0L, 0L,
                            duringTime, ts
                    );

                });

        // 2. uv
        SingleOutputStreamOperator<VisitorStats> uvStream = topicStreamMap.get(TOPIC_DWM_UV)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    Long ts = obj.getLong("ts");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, is_new,
                            1L, 0L, 0L, 0L, 0L, ts
                    );
                });
        //3.uj
        SingleOutputStreamOperator<VisitorStats> ujStream = topicStreamMap.get(DWM_UJ)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    Long ts = obj.getLong("ts");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, is_new,
                            0L, 0L, 0L, 1L, 0L, ts
                    );
                });
        // 4. sv 进入 来源的topic? 怎么判断是进入?
        // filter+map    flatMap   process
        SingleOutputStreamOperator<VisitorStats> svStream = topicStreamMap
                .get(TOPIC_DWD_PAGE)
                .flatMap(new FlatMapFunction<String, VisitorStats>() {
                    @Override
                    public void flatMap(String json,
                                        Collector<VisitorStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(json);
                        String lastPageId = obj.getJSONObject("page").getString("last_page_id");

                        if (lastPageId == null || lastPageId.length() == 0) {
                            JSONObject common = obj.getJSONObject("common");
                            String vc = common.getString("vc");
                            String ch = common.getString("ch");
                            String ar = common.getString("ar");
                            String is_new = common.getString("is_new");
                            Long ts = obj.getLong("ts");

                            VisitorStats vs = new VisitorStats(
                                    "", "",
                                    vc, ch, ar, is_new,
                                    0L, 0L, 1L, 0L, 0L, ts
                            );
                            out.collect(vs);
                        }
                    }
                });
         return pvAndDtStream.union(uvStream, ujStream, svStream);
    }
}
/*
pv  dwd_page
uv  dwm_uv
uj  dwm_uj

-----------
得到三个流
pvStream
uvStrean
ujStream

------
从 pvStream
渠道  版本  pv
 小米  1.1  1
 小米  1.1  1
uvStrean
 渠道  版本  uv
 小米  1.1   1
 ...

 ujStream
渠道  版本  uj
小米   1.1  1
...
如何得到?
union  数据类型要一致
union前各个流的数据类型要一致, 和union后的类型要一致

渠道  版本   pv  uv  uj
小米  1.1    1   0   0
小米  1.1    0   1   0
小米  1.1    0   0   1


-------------------
渠道  版本   pv  uv  uj
小米   1.1   1   0   0
小米   1.1   1   0   0
....
keyBy->开窗-> 聚合
----
最结果:
窗口  渠道  版本   pv  uv  uj
0-5  小米   1.1   10  2   1
0-5  华为   1.1   11  5   0
....
 */