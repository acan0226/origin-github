package net.acan.gmall.realtime.Function;

import net.acan.gmall.realtime.util.MyUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

//row是弱类型,需要用注解告诉Flink每一列的名字和类型是什么
@FunctionHint(output = @DataTypeHint("row<word string>"))
public class IkAnalyzer extends TableFunction<Row> {
    public void eval(String kw) throws IOException {//约定好自定义函数要使用eval方法
        // 把kw 进行切词   小米手机 -> 小米 手机
        List<String> words = MyUtil.split(kw);
        for (String word : words) {
            collect(Row.of(word));
        }
    }
}
