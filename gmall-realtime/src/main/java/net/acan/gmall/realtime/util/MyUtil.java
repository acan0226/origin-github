package net.acan.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.ibm.icu.text.SimpleDateFormat;
import net.acan.gmall.realtime.annotation.NotSink;
import net.acan.gmall.realtime.bean.ProductStats;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class MyUtil {
    public static <T> List<T> toList(Iterable<T> it) {
        List<T> result = new ArrayList<>();
        for (T t : it) {
            result.add(t);
        }
        return result;
    }

    public static String toDateTime(Long ts, String... format) {

        String f = "yyyy-MM-dd HH:mm:ss";
        if (format.length != 0) {  // 没有传递时间格式, 使用一个默认格式
            f = format[0];
        }
        return new SimpleDateFormat(f).format(ts);
    }

    public static <T> String getFieldString(Class<T> tClass) {
        String[] fieldNames = getFields(tClass);//存储了所有的属性名
        StringBuilder s = new StringBuilder();
        for (String fieldName : fieldNames) {
            s.append(fieldName).append(",");
        }
        s.deleteCharAt(s.length()-1);
        return s.toString();
    }

    public static void main(String[] args) {
        System.out.println(getFieldString(ProductStats.class));
    }

    public static <T> String[] getFields(Class<T> tClass) {
        Field[] fields = tClass.getDeclaredFields();
        /*
        String[] result = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = fields[i].getName();
        }
        return result;
    */
        List<String> names = new ArrayList<>();
        for (Field field : fields) {
            // 对那些不需要sink的 field应该过滤掉
            //            if(field.getName().equals("orderIdSet") || ..)  // 这样写灵活度太低了
            NotSink noSink = field.getAnnotation(NotSink.class);
            if (noSink == null) { // 没有注解,表示这个属性要写出去
                names.add(field.getName());
            }

        }
        return names.toArray(new String[0]);
    }



    public static Long toTs(String dateTime, String... format) throws ParseException {
        String f = "yyyy-MM-dd HH:mm:ss";
        if (format.length != 0) {  // 没有传递时间格式, 使用一个默认格式
            f = format[0];
        }
        return new com.ibm.icu.text.SimpleDateFormat(f).parse(dateTime).getTime();
    }
}
