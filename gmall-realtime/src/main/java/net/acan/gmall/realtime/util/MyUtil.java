package net.acan.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
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

    public static String toDateTime(Long ts,String ... format){
       String f = "yyyy-MM-dd HH:mm:ss";
        if (format.length != 0) {// 没有传递时间格式, 使用一个默认格式
            f=format[0];
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

    public static <T> String[] getFields(Class<T> tClass) {
        Field[] fields = tClass.getDeclaredFields();
        String[] result = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = fields[i].getName();
        }
        return result;
    }


}
