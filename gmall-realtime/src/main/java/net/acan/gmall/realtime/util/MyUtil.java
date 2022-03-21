package net.acan.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;

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
}
