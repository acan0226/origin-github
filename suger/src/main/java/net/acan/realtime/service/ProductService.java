package net.acan.realtime.service;

import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ProductService {

    BigDecimal gmv(int date);

    List<Map<String,Object>> gmvByTm(int date);
}
