package net.acan.gmall.realtime.app.dwd;

import net.acan.gmall.realtime.app.BaseAppV1;
import net.acan.gmall.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdLogApp extends BaseAppV1 {
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
        stream.print();
    }
}
