package net.acan.realtime.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import net.acan.realtime.bean.Province;
import net.acan.realtime.service.ProductService;
import net.acan.realtime.service.ProvinceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
public class SugerController {

    @Autowired
    ProductService productService;
    @Autowired
    ProvinceService provinceService;

    @RequestMapping("/gmv")
    public String gmv(@RequestParam("date") int date) {
        BigDecimal gmv = productService.gmv(date);
        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");
        result.put("data", gmv);

        return result.toJSONString();
    }
    @RequestMapping("/gmv/tm")
    public String gmvByTm(@RequestParam("date") int date) {
        List<Map<String, Object>> list = productService.gmvByTm(date);

        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        for (Map<String, Object> tm : list) {
            Object tm_name = tm.get("tm_name");
            categories.add(tm_name);

        }
        data.put("categories", categories);

        JSONArray series = new JSONArray();

        JSONObject obj = new JSONObject();
        obj.put("name", "商品品牌");
        JSONArray data1 = new JSONArray();


        for (Map<String, Object> tm : list) {
            Object order_amount = tm.get("order_amount");
            data1.add(order_amount);
        }
        obj.put("data", data1);
        series.add(obj);


        data.put("series", series);

        result.put("data", data);

        return result.toJSONString();


    }

    @RequestMapping("/province")
    public String statProvince(@RequestParam("date") int date){
        List<Province> list = provinceService.statProvince(date);
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();

        JSONArray mapData = new JSONArray();

        for (Province province : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", province.getProvince_name());
            obj.put("value", province.getOrder_amount());

            JSONArray tooltipValues = new JSONArray();
            tooltipValues.add(province.getOrder_count());
            obj.put("tooltipvalues", tooltipValues);

            mapData.add(obj);
        }
       data.put("mapData", mapData);
        data.put("valueName", "销售额");

        JSONArray tooltipNames = new JSONArray();
        tooltipNames.add("订单数");
        JSONArray tooltipUnits = new JSONArray();
        tooltipUnits.add("个");
        data.put("tooltipNames", tooltipNames);
        data.put("tooltipUnits", tooltipUnits);

        result.put("data", data);


        return result.toJSONString();
    }
}
